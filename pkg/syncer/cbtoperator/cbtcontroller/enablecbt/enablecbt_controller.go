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

package enablecbt

import (
	"context"
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
)

// Add creates a new EnableCBT Controller and adds it to the Manager.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor, volumeManager volume.Manager) error {
	_, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the EnableCBT Controller as its a non-WCP CSI deployment")
		return nil
	}
	return add(mgr, newReconciler(mgr, volumeManager))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, volumeManager volume.Manager) reconcile.Reconciler {
	return &ReconcileEnableCBT{client: mgr.GetClient(), scheme: mgr.GetScheme(), volumeManager: volumeManager}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	_, log := logger.GetNewContextWithLogger()
	// Create a new controller.
	c, err := controller.New("enablecbt-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new EnableCBT controller with error: %+v", err)
		return err
	}

	pred := predicate.TypedFuncs[*cnsdpv1alpha1.EnableCBT]{
		CreateFunc: func(e event.TypedCreateEvent[*cnsdpv1alpha1.EnableCBT]) bool {
			return e.Object.Status.Cbt
		},
		UpdateFunc: func(e event.TypedUpdateEvent[*cnsdpv1alpha1.EnableCBT]) bool {
			// Trigger if Cbt changes
			if e.ObjectOld.Status.Cbt != e.ObjectNew.Status.Cbt {
				return true
			}
			return false
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*cnsdpv1alpha1.EnableCBT]) bool {
			return false
		},
	}

	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&cnsdpv1alpha1.EnableCBT{},
		&handler.TypedEnqueueRequestForObject[*cnsdpv1alpha1.EnableCBT]{}, pred))
	if err != nil {
		log.Errorf("failed to watch for changes to EnableCBT resource with error: %+v", err)
		return err
	}
	return nil
}

// ReconcileEnableCBT reconciles a EnableCBT object.
type ReconcileEnableCBT struct {
	client        client.Client
	scheme        *runtime.Scheme
	volumeManager volume.Manager
}

// Reconcile reads that state of the cluster for a EnableCBT object and makes changes.
func (r *ReconcileEnableCBT) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for request: %q", request.NamespacedName)

	instance := &cnsdpv1alpha1.EnableCBT{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			reconcileLog.Info("EnableCBT resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		reconcileLog.Errorf("Error reading the EnableCBT with name: %q. Err: %+v", request.Name, err)
		return reconcile.Result{}, err
	}

	if instance.DeletionTimestamp != nil {
		return reconcile.Result{}, nil
	}

	if instance.Status.Cbt {
		return r.enableCBTForNamespace(ctx, instance)
	} else {
		return r.disableCBTForNamespace(ctx, instance)
	}
}

func (r *ReconcileEnableCBT) enableCBTForNamespace(ctx context.Context, instance *cnsdpv1alpha1.EnableCBT) (reconcile.Result, error) {
	reconcileLog := logger.GetLogger(ctx)

	// 1. List all PVCs in the namespace without label cbt=true
	labelSelector, err := labels.Parse("cbt!=true")
	if err != nil {
		reconcileLog.Errorf("Failed to parse label selector. Err: %+v", err)
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	var pvcList v1.PersistentVolumeClaimList
	err = r.client.List(ctx, &pvcList, &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	})
	if err != nil {
		reconcileLog.Errorf("Failed to list PVCs in namespace: %q. Err: %+v", instance.Namespace, err)
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	reconcileLog.Infof("Found %d PVCs without label cbt=true in namespace: %q", len(pvcList.Items), instance.Namespace)

	// 2. Find unattached PVCs
	// Get all VolumeAttachments to efficiently check if attached
	attachedPVs, err := r.getAttachedPVs(ctx)
	if err != nil {
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	for _, pvc := range pvcList.Items {
		if pvc.Spec.VolumeName == "" {
			continue // Unbound PVC
		}

		if attachedPVs[pvc.Spec.VolumeName] {
			continue // PVC is attached
		}

		// Find the PV to get volume ID
		var pv v1.PersistentVolume
		err = r.client.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
		if err != nil {
			reconcileLog.Errorf("Failed to get PV %s for PVC %s. Err: %+v", pvc.Spec.VolumeName, pvc.Name, err)
			continue
		}

		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != common.VSphereCSIDriverName {
			continue
		}

		if pv.Spec.CSI.VolumeAttributes == nil || pv.Spec.CSI.VolumeAttributes[common.AttributeDiskType] != common.DiskTypeBlockVolume {
			continue // Not a vSphere CNS block volume
		}

		volumeID := pv.Spec.CSI.VolumeHandle
		if volumeID == "" {
			continue
		}

		reconcileLog.Infof("Enabling CBT for unattached PVC %s (Volume ID: %s)", pvc.Name, volumeID)
		err = common.SetVolumeCbtFlagsUtil(ctx, r.volumeManager, volumeID)
		if err != nil {
			// ignore error now as we are not blocking the reconciliation
			reconcileLog.Errorf("Failed to enable CBT for volume %s. Err: %+v", volumeID, err)
			continue
		}
		// Add label cbt=true to the PVC
		if pvc.Labels == nil {
			pvc.Labels = make(map[string]string)
		}
		pvc.Labels["cbt"] = "true"
		err = r.client.Update(ctx, &pvc)
		if err != nil {
			reconcileLog.Errorf("Failed to add label cbt=true to PVC %s. Err: %+v", pvc.Name, err)
			continue
		}
	}

	reconcileLog.Infof("Finished EnableCBT Reconcile for namespace: %q", instance.Namespace)
	return reconcile.Result{}, nil
}

func (r *ReconcileEnableCBT) disableCBTForNamespace(ctx context.Context, instance *cnsdpv1alpha1.EnableCBT) (reconcile.Result, error) {
	reconcileLog := logger.GetLogger(ctx)

	// 1. List all PVCs in the namespace with label cbt=true
	labelSelector, err := labels.Parse("cbt=true")
	if err != nil {
		reconcileLog.Errorf("Failed to parse label selector. Err: %+v", err)
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	var pvcList v1.PersistentVolumeClaimList
	err = r.client.List(ctx, &pvcList, &client.ListOptions{
		Namespace:     instance.Namespace,
		LabelSelector: labelSelector,
	})
	if err != nil {
		reconcileLog.Errorf("Failed to list PVCs in namespace: %q. Err: %+v", instance.Namespace, err)
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	reconcileLog.Infof("Found %d PVCs with label cbt=true in namespace: %q", len(pvcList.Items), instance.Namespace)

	// 2. Find unattached PVCs
	// Get all VolumeAttachments to efficiently check if attached
	attachedPVs, err := r.getAttachedPVs(ctx)
	if err != nil {
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	for _, pvc := range pvcList.Items {
		if pvc.Spec.VolumeName == "" {
			continue // Unbound PVC
		}

		if attachedPVs[pvc.Spec.VolumeName] {
			continue // PVC is attached
		}

		// Find the PV to get volume ID
		var pv v1.PersistentVolume
		err = r.client.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, &pv)
		if err != nil {
			reconcileLog.Errorf("Failed to get PV %s for PVC %s. Err: %+v", pvc.Spec.VolumeName, pvc.Name, err)
			continue
		}

		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != common.VSphereCSIDriverName {
			continue
		}

		if pv.Spec.CSI.VolumeAttributes == nil || pv.Spec.CSI.VolumeAttributes[common.AttributeDiskType] != common.DiskTypeBlockVolume {
			continue // Not a vSphere CNS block volume
		}

		volumeID := pv.Spec.CSI.VolumeHandle
		if volumeID == "" {
			continue
		}

		reconcileLog.Infof("Disabling CBT for unattached PVC %s (Volume ID: %s)", pvc.Name, volumeID)
		err = common.ClearVolumeCbtFlagsUtil(ctx, r.volumeManager, volumeID)
		if err != nil {
			// ignore error now as we are not blocking the reconciliation
			reconcileLog.Errorf("Failed to disable CBT for volume %s. Err: %+v", volumeID, err)
			continue
		}

		// Remove label cbt=true from the PVC
		delete(pvc.Labels, "cbt")
		err = r.client.Update(ctx, &pvc)
		if err != nil {
			reconcileLog.Errorf("Failed to remove label cbt=true from PVC %s. Err: %+v", pvc.Name, err)
			continue
		}
	}

	reconcileLog.Infof("Finished DisableCBT Reconcile for namespace: %q", instance.Namespace)
	return reconcile.Result{}, nil
}

func (r *ReconcileEnableCBT) getAttachedPVs(ctx context.Context) (map[string]bool, error) {
	reconcileLog := logger.GetLogger(ctx)
	var vaList storagev1.VolumeAttachmentList
	err := r.client.List(ctx, &vaList)
	if err != nil {
		reconcileLog.Errorf("Failed to list VolumeAttachments. Err: %+v", err)
		return nil, err
	}

	attachedPVs := make(map[string]bool)
	for _, va := range vaList.Items {
		if va.Spec.Source.PersistentVolumeName != nil {
			attachedPVs[*va.Spec.Source.PersistentVolumeName] = true
		}
	}
	return attachedPVs, nil
}
