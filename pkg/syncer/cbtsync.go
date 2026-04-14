/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package syncer

import (
	"context"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	cnsdpv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsdp/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var enableCBTResource = schema.GroupVersionResource{
	Group:    "cnsdp.vmware.com",
	Version:  "v1alpha1",
	Resource: "enablecbts",
}

const cbtLabelKey = "cbt"

// runPeriodicCBTSync reconciles PVC cbt labels with CNS changed-block-tracking state.
// It is invoked on the interval configured by CBT_SYNC_INTERVAL_MINUTES (see getCBTSyncIntervalInMin).
// InitMetadataSyncer starts the periodic caller only on Supervisor when supports_CSI_Backup_API is
// enabled at startup. This function no-ops if no EnableCBT CR exists in the cluster.
func runPeriodicCBTSync(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	namespaces, err := listNamespacesWithEnableCBTCR(ctx)
	if err != nil {
		log.Errorf("CBTSync: failed to list EnableCBT CRs: %v", err)
		return
	}
	if len(namespaces) == 0 {
		log.Debug("CBTSync: skipping, no EnableCBT CR in cluster")
		return
	}
	log.Infof("CBTSync: starting periodic reconciliation for %d namespace(s)", len(namespaces))
	if err := csiCBTSync(ctx, metadataSyncer, metadataSyncer.configInfo.Cfg.Global.VCenterIP, namespaces); err != nil {
		log.Errorf("CBTSync: reconciliation failed: %v", err)
	}
}

func listNamespacesWithEnableCBTCR(ctx context.Context) (map[string]struct{}, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	unstructuredList, err := dynClient.Resource(enableCBTResource).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var list cnsdpv1alpha1.EnableCBTList
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredList.UnstructuredContent(), &list); err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for i := range list.Items {
		out[list.Items[i].Namespace] = struct{}{}
	}
	return out, nil
}

func csiCBTSync(ctx context.Context, metadataSyncer *metadataSyncInformer, vc string,
	namespaces map[string]struct{}) error {
	log := logger.GetLogger(ctx)
	volManager, err := getVolManagerForVcHost(ctx, vc, metadataSyncer)
	if err != nil {
		return err
	}
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		return err
	}
	for ns := range namespaces {
		syncPVCLabelsWithCBTInNamespace(ctx, volManager, k8sClient, ns)
	}
	log.Infof("CBTSync: finished periodic reconciliation for VC %s", vc)
	return nil
}

func syncPVCLabelsWithCBTInNamespace(ctx context.Context, volManager volumes.Manager,
	k8sClient clientset.Interface, namespace string) {
	log := logger.GetLogger(ctx)
	pvcList, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("CBTSync: failed to list PVCs in namespace %q: %v", namespace, err)
		return
	}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if pvc.Spec.VolumeName == "" {
			continue
		}
		pv, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			log.Warnf("CBTSync: failed to get PV %q for PVC %q: %v", pvc.Spec.VolumeName, pvc.Name, err)
			continue
		}
		if !isCBTSyncEligibleVolume(pv) {
			continue
		}
		volumeID := pv.Spec.CSI.VolumeHandle
		queryRes, err := volManager.QueryVolume(ctx, cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
		})
		if err != nil {
			log.Warnf("CBTSync: QueryVolume failed for volume %q (PVC %q): %v", volumeID, pvc.Name, err)
			continue
		}
		if len(queryRes.Volumes) == 0 {
			log.Warnf("CBTSync: no CNS volume for volume %q (PVC %q)", volumeID, pvc.Name)
			continue
		}
		cbtOn := queryRes.Volumes[0].ChangedBlockTracking == cnstypes.CnsVolumeCBTStatusEnabled
		labelSaysCBT := pvc.Labels != nil && pvc.Labels[cbtLabelKey] == "true"
		if cbtOn == labelSaysCBT {
			continue
		}
		fresh, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			log.Warnf("CBTSync: failed to re-get PVC %q: %v", pvc.Name, err)
			continue
		}
		freshLabelCBT := fresh.Labels != nil && fresh.Labels[cbtLabelKey] == "true"
		if freshLabelCBT == cbtOn {
			continue
		}
		toUpdate := fresh.DeepCopy()
		if toUpdate.Labels == nil {
			toUpdate.Labels = map[string]string{}
		}
		if cbtOn {
			toUpdate.Labels[cbtLabelKey] = "true"
			log.Infof("CBTSync: setting %s=%s on PVC %s/%s (CNS CBT enabled)", cbtLabelKey, "true", namespace, pvc.Name)
		} else {
			delete(toUpdate.Labels, cbtLabelKey)
			log.Infof("CBTSync: removing label %s from PVC %s/%s (CNS CBT disabled)", cbtLabelKey, namespace, pvc.Name)
		}
		if _, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, toUpdate, metav1.UpdateOptions{}); err != nil {
			log.Errorf("CBTSync: failed to update PVC %s/%s: %v", namespace, pvc.Name, err)
		}
	}
}

func isCBTSyncEligibleVolume(pv *v1.PersistentVolume) bool {
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != common.VSphereCSIDriverName {
		return false
	}
	if pv.Spec.CSI.VolumeAttributes == nil ||
		pv.Spec.CSI.VolumeAttributes[common.AttributeDiskType] != common.DiskTypeBlockVolume {
		return false
	}
	return pv.Spec.CSI.VolumeHandle != ""
}
