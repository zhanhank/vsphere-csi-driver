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

package cbtconfig

import (
	"context"
	"strings"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cnsdpv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsdp/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	maxWorkerThreads = 10
	// cbtConfigListPageSize caps list responses per apiserver call (namespaced PVCs and cluster VolumeAttachments).
	cbtConfigListPageSize int64 = 500
)

// cbtPVCReconcileParams holds derived strings and selectors for one enable/disable reconcile pass.
type cbtPVCReconcileParams struct {
	labelSelector labels.Selector
	pvcListDesc   string
	actionVerb    string
	failVerb      string
	finishTag     string
	enable        bool
}

func newCBTPVCReconcileParams(enable bool) (*cbtPVCReconcileParams, error) {
	p := &cbtPVCReconcileParams{enable: enable}
	if enable {
		p.pvcListDesc = "without label cbt=true"
		p.actionVerb = "Enabling"
		p.failVerb = "enable"
		p.finishTag = "enable"
	} else {
		p.pvcListDesc = "with label cbt=true"
		p.actionVerb = "Disabling"
		p.failVerb = "disable"
		p.finishTag = "disable"
	}
	sel := "cbt!=true"
	if !enable {
		sel = "cbt=true"
	}
	ls, err := labels.Parse(sel)
	if err != nil {
		return nil, err
	}
	p.labelSelector = ls
	return p, nil
}

func cbtStatusReportsEnabled(st cnsdpv1alpha1.CBTConfigStatus) bool {
	return st.Enabled != nil && *st.Enabled
}

// Add creates a new CBTConfig controller and adds it to the Manager.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor, volumeManager volume.Manager) error {
	_, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CBTConfig controller as its a non-WCP CSI deployment")
		return nil
	}
	return add(mgr, newReconciler(mgr, volumeManager))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, volumeManager volume.Manager) reconcile.Reconciler {
	return &ReconcileCBTConfig{client: mgr.GetClient(), scheme: mgr.GetScheme(), volumeManager: volumeManager}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()
	// Create a new controller.
	c, err := controller.New("cbtconfig-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new CBTConfig controller with error: %+v", err)
		return err
	}

	pred := predicate.TypedFuncs[*cnsdpv1alpha1.CBTConfig]{
		CreateFunc: func(e event.TypedCreateEvent[*cnsdpv1alpha1.CBTConfig]) bool {
			return cbtStatusReportsEnabled(e.Object.Status)
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*cnsdpv1alpha1.CBTConfig]) bool {
			return cbtStatusReportsEnabled(e.ObjectOld.Status) != cbtStatusReportsEnabled(e.ObjectNew.Status)
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*cnsdpv1alpha1.CBTConfig]) bool {
			return false
		},
	}

	// source.Kind does not talk to the apiserver here: Watch only registers the source.
	// When the manager starts the controller, controller-runtime's Kind source polls
	// cache.GetInformer for this type (default 10s) until the CBTConfig CRD is registered
	// or the manager context is cancelled, then attaches the informer handler.
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&cnsdpv1alpha1.CBTConfig{},
		&handler.TypedEnqueueRequestForObject[*cnsdpv1alpha1.CBTConfig]{}, pred))
	if err != nil {
		log.Errorf("failed to watch for changes to CBTConfig resource with error: %+v", err)
		return err
	}
	return nil
}

// ReconcileCBTConfig reconciles a CBTConfig object.
type ReconcileCBTConfig struct {
	client        client.Client
	scheme        *runtime.Scheme
	volumeManager volume.Manager
}

// Reconcile reads that state of the cluster for a CBTConfig object and makes changes.
func (r *ReconcileCBTConfig) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for request: %q", request.NamespacedName)

	instance := &cnsdpv1alpha1.CBTConfig{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Info("CBTConfig resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		reconcileLog.Errorf("Error reading the CBTConfig with name: %q. Err: %+v", request.Name, err)
		return reconcile.Result{}, err
	}

	if instance.DeletionTimestamp != nil {
		return reconcile.Result{}, nil
	}

	return r.reconcileCBTForNamespace(ctx, instance, cbtStatusReportsEnabled(instance.Status))
}

// reconcileCBTForNamespace lists unattached vSphere block PVCs in the namespace (by label
// selector), sets or clears CBT on the backing volume, then adds or removes the cbt=true
// PVC label so attached volumes are handled on a later reconcile.
func (r *ReconcileCBTConfig) reconcileCBTForNamespace(ctx context.Context, instance *cnsdpv1alpha1.CBTConfig, enable bool) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	params, err := newCBTPVCReconcileParams(enable)
	if err != nil {
		log.Errorf("Failed to parse label selector. Err: %+v", err)
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}
	attachedPVs, err := r.getAttachedPVs(ctx)
	if err != nil {
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}
	totalPVCsListed, err := r.reconcileCBTForAllPVCPages(ctx, instance.Namespace, params, attachedPVs)
	if err != nil {
		log.Errorf("Failed to list PVCs in namespace: %q. Err: %+v", instance.Namespace, err)
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}
	log.Infof("Listed %d PVCs %s in namespace: %q (paged)", totalPVCsListed, params.pvcListDesc, instance.Namespace)
	log.Infof("Finished CBTConfig reconcile (%s) for namespace: %q", params.finishTag, instance.Namespace)
	return reconcile.Result{}, nil
}

func (r *ReconcileCBTConfig) reconcileCBTForAllPVCPages(ctx context.Context, namespace string,
	params *cbtPVCReconcileParams, attachedPVs map[string]bool) (int, error) {
	listOpts := &client.ListOptions{
		Namespace:     namespace,
		LabelSelector: params.labelSelector,
		Limit:         cbtConfigListPageSize,
	}
	var totalPVCsListed int
	for {
		var pvcList v1.PersistentVolumeClaimList
		if err := r.client.List(ctx, &pvcList, listOpts); err != nil {
			return 0, err
		}
		totalPVCsListed += len(pvcList.Items)
		r.processCBTForPVCCandidatesInPage(ctx, &pvcList, params, attachedPVs)
		if pvcList.Continue == "" {
			break
		}
		listOpts.Continue = pvcList.Continue
	}
	return totalPVCsListed, nil
}

func (r *ReconcileCBTConfig) processCBTForPVCCandidatesInPage(ctx context.Context, pvcList *v1.PersistentVolumeClaimList,
	params *cbtPVCReconcileParams, attachedPVs map[string]bool) {
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !r.pvcShouldBeConsideredForCBT(ctx, pvc, attachedPVs) {
			continue
		}
		r.tryApplyCBTAndLabelForPVC(ctx, pvc, params)
	}
}

func (r *ReconcileCBTConfig) pvcShouldBeConsideredForCBT(ctx context.Context, pvc *v1.PersistentVolumeClaim, attachedPVs map[string]bool) bool {
	log := logger.GetLogger(ctx)
	if pvc.Spec.VolumeName == "" {
		return false
	}
	if attachedPVs[pvc.Spec.VolumeName] {
		log.Debugf("PVC %s is attached to PodVM", pvc.Name)
		return false
	}
	if pvc.Annotations != nil {
		for key := range pvc.Annotations {
			if strings.HasPrefix(key, "cns.vmware.com/usedby-vm-") {
				log.Debugf("PVC %s is attached to a VM Service VM", pvc.Name)
				return false
			}
		}
	}
	return true
}

func (r *ReconcileCBTConfig) tryApplyCBTAndLabelForPVC(ctx context.Context, pvc *v1.PersistentVolumeClaim, params *cbtPVCReconcileParams) {
	log := logger.GetLogger(ctx)
	var pv v1.PersistentVolume
	if err := r.client.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv); err != nil {
		log.Errorf("Failed to get PV %s for PVC %s. Err: %+v", pvc.Spec.VolumeName, pvc.Name, err)
		return
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != common.VSphereCSIDriverName {
		return
	}
	if pv.Spec.CSI.VolumeAttributes == nil || pv.Spec.CSI.VolumeAttributes[common.AttributeDiskType] != common.DiskTypeBlockVolume {
		return
	}
	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		return
	}
	log.Infof("%s CBT for unattached PVC %s (Volume ID: %s)", params.actionVerb, pvc.Name, volumeID)
	var err error
	if params.enable {
		err = common.SetVolumeCbtFlagsUtil(ctx, r.volumeManager, volumeID)
	} else {
		err = common.ClearVolumeCbtFlagsUtil(ctx, r.volumeManager, volumeID)
	}
	if err != nil {
		log.Errorf("Failed to %s CBT for volume %s. Err: %+v", params.failVerb, volumeID, err)
		return
	}
	if params.enable {
		if pvc.Labels == nil {
			pvc.Labels = make(map[string]string)
		}
		pvc.Labels["cbt"] = "true"
	} else {
		delete(pvc.Labels, "cbt")
	}
	if err := r.client.Update(ctx, pvc); err != nil {
		if params.enable {
			log.Errorf("Failed to add label cbt=true to PVC %s. Err: %+v", pvc.Name, err)
		} else {
			log.Errorf("Failed to remove label cbt=true from PVC %s. Err: %+v", pvc.Name, err)
		}
	}
}

// getAttachedPVs returns PV names that have an active VolumeAttachment (cluster-scoped list, paged).
func (r *ReconcileCBTConfig) getAttachedPVs(ctx context.Context) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	attachedPVs := make(map[string]bool)
	listOpts := &client.ListOptions{Limit: cbtConfigListPageSize}
	for {
		var vaList storagev1.VolumeAttachmentList
		if err := r.client.List(ctx, &vaList, listOpts); err != nil {
			log.Errorf("Failed to list VolumeAttachments. Err: %+v", err)
			return nil, err
		}
		for i := range vaList.Items {
			va := &vaList.Items[i]
			if va.Spec.Source.PersistentVolumeName != nil {
				attachedPVs[*va.Spec.Source.PersistentVolumeName] = true
			}
		}
		if vaList.Continue == "" {
			break
		}
		listOpts.Continue = vaList.Continue
	}
	return attachedPVs, nil
}
