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

package cbtoperator

import (
	"context"
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cbtoperator/cbtcontroller"
)

// NewManager creates and initializes a new controller manager for the specified
// cluster flavor.
func NewManager(
	ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo,
) (ctrlmgr.Manager, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Initializing CBT Operator")
	kubeConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	mgr, err := ctrlmgr.New(kubeConfig, ctrlmgr.Options{
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create new cbtoperator instance. Err: %+v", err)
	}

	log.Info("Registering Components for CBT Operator")

	// Setup Scheme for all resources
	if err := cbtconfigv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, fmt.Errorf("failed to set scheme for CBT operator for type %s. Err: %+v", cbtconfigv1alpha1.GroupVersion.String(), err)
	}

	vcClient, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
	if err != nil {
		return nil, err
	}

	volumeManager, err := volume.GetManager(ctx, vcClient, nil, false,
		false, false, clusterFlavor, configInfo.Cfg.Global.SupervisorID, configInfo.Cfg.Global.ClusterDistribution)
	if err != nil {
		return nil, fmt.Errorf("failed to create an instance of volume manager: %w", err)
	}

	if err := cbtcontroller.AddToManager(mgr, clusterFlavor, volumeManager); err != nil {
		return nil, err
	}

	return mgr, nil
}
