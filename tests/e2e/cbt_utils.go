/*
Copyright 2026 The Kubernetes Authors.

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

package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vslm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// cbtConfigGVR is the GroupVersionResource for the CBTConfig CR. It is
// installed on the Supervisor by the Data Protection (DP) operator and is
// not present by default on a stock supervisor cluster, even when the WCP
// `CSI_Backup_API` capability is enabled.
var cbtConfigGVR = cbtconfigv1alpha1.GroupVersion.WithResource(cbtconfigv1alpha1.CBTConfigResource)

// cbtPollInterval is the poll interval used while waiting for the CBT flag
// or CBTConfig.Status.Enabled to converge. The attach path applies the CBT
// flag asynchronously, so callers must always poll instead of asserting once.
const cbtPollInterval = 5 * time.Second

// cbtConvergeTimeout bounds how long we wait for an FCD CBT flag transition
// or CBTConfig status to reach the requested value. It is intentionally
// shorter than pollTimeout — these state machines reconcile at ~reconcile
// period speed, not at provisioning speed.
const cbtConvergeTimeout = 3 * time.Minute

// cbtFlagSteadyWindow is the time we wait when asserting that the CBT flag
// stays at a particular value (the "no CBTConfig CR" case in test #1).
// Long enough to step over a controller reconcile cycle, short enough not to
// drag the suite.
const cbtFlagSteadyWindow = 30 * time.Second

// isCBTConfigCRDInstalledOnSupervisor returns true if the CBTConfig CRD is
// discoverable on the Supervisor identified by svcRestCfg. The CRD is owned
// by the DP operator; its absence is the canonical "skip CBT tests" signal
// for both Supervisor and VKS flavors.
func isCBTConfigCRDInstalledOnSupervisor(ctx context.Context, svcRestCfg *rest.Config) bool {
	dyn, err := dynamic.NewForConfig(svcRestCfg)
	if err != nil {
		framework.Logf("isCBTConfigCRDInstalledOnSupervisor: dynamic client error: %v", err)
		return false
	}
	// A List against the GVR is the cheapest discovery probe that surfaces
	// IsNoMatchError when the CRD is absent, while still working when no CRs
	// exist.
	_, err = dyn.Resource(cbtConfigGVR).Namespace("").List(ctx, metav1.ListOptions{Limit: 1})
	if err == nil {
		return true
	}
	if apimeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
		return false
	}
	// Unexpected error — log and treat as missing so the test skips loud-
	// failing setup issues. (We do not gomega.Expect here so the helper can
	// be used inside Skip() without aborting the spec.)
	framework.Logf("isCBTConfigCRDInstalledOnSupervisor: unexpected list error: %v", err)
	return false
}

// isCBTBackupAPIFssEnabledOnGuest returns true if the guest pvCSI has its
// CSI_Backup_API FSS enabled in the internal-feature-states ConfigMap. Used
// by the VKS test path only; combine with isCBTConfigCRDInstalledOnSupervisor
// because the CR lives on the Supervisor.
func isCBTBackupAPIFssEnabledOnGuest(ctx context.Context, guestClient clientset.Interface) bool {
	return isCsiFssEnabled(ctx, guestClient, csiSystemNamespace, common.CSI_Backup_API_FSS)
}

// newSupervisorDynamicClient returns a dynamic client wired to the Supervisor
// kubeconfig referenced by SUPERVISOR_CLUSTER_KUBE_CONFIG, alongside the
// underlying *rest.Config (callers typically need both).
func newSupervisorDynamicClient() (dynamic.Interface, *rest.Config) {
	cfg := getSupervisorRestConfig()
	dyn, err := dynamic.NewForConfig(cfg)
	framework.ExpectNoError(err, "failed to build dynamic client for supervisor")
	return dyn, cfg
}

// getSupervisorRestConfig builds a *rest.Config from
// SUPERVISOR_CLUSTER_KUBE_CONFIG. Same kubeconfig env used by every other
// supervisor-side helper (e.g. `getCnsNodeVMAttachmentByName`).
func getSupervisorRestConfig() *rest.Config {
	k8senv := GetAndExpectStringEnvVar("SUPERVISOR_CLUSTER_KUBE_CONFIG")
	cfg, err := clientcmd.BuildConfigFromFlags("", k8senv)
	framework.ExpectNoError(err, "failed to build rest config from %s", k8senv)
	return cfg
}

// setCBTConfig creates or updates the namespace-scoped CBTConfig CR named
// "default" with spec.enabled=enabled, then waits for status.enabled to
// reflect the requested value.
//
// CBTConfig is a namespace singleton. The DP operator reconciles status from
// spec; until status.enabled matches, callers should not assert FCD CBT flag
// behavior because the CSI controllers gate on status.enabled, not spec.
func setCBTConfig(
	ctx context.Context,
	dyn dynamic.Interface,
	namespace string,
	enabled bool,
) {
	const crName = "default"
	res := dyn.Resource(cbtConfigGVR).Namespace(namespace)

	existing, err := res.Get(ctx, crName, metav1.GetOptions{})
	switch {
	case err == nil:
		framework.Logf("setCBTConfig: updating CBTConfig %s/%s spec.enabled=%t", namespace, crName, enabled)
		if err := unstructured.SetNestedField(existing.Object, enabled, "spec", "enabled"); err != nil {
			framework.Failf("failed to patch CBTConfig %s/%s spec: %v", namespace, crName, err)
		}
		_, err = res.Update(ctx, existing, metav1.UpdateOptions{})
		framework.ExpectNoError(err, "failed to update CBTConfig %s/%s", namespace, crName)
	case apierrors.IsNotFound(err):
		framework.Logf("setCBTConfig: creating CBTConfig %s/%s spec.enabled=%t", namespace, crName, enabled)
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(cbtconfigv1alpha1.GroupVersion.WithKind("CBTConfig"))
		obj.SetName(crName)
		obj.SetNamespace(namespace)
		if err := unstructured.SetNestedField(obj.Object, enabled, "spec", "enabled"); err != nil {
			framework.Failf("failed to set CBTConfig %s/%s spec: %v", namespace, crName, err)
		}
		_, err = res.Create(ctx, obj, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create CBTConfig %s/%s", namespace, crName)
	default:
		framework.Failf("failed to get CBTConfig %s/%s: %v", namespace, crName, err)
	}

	waitForCBTConfigStatusEnabled(ctx, dyn, namespace, enabled)
}

// deleteCBTConfig deletes the namespace-scoped CBTConfig CR. Best-effort:
// `IsNotFound` is tolerated so the call is safe to use in `defer` paths.
func deleteCBTConfig(ctx context.Context, dyn dynamic.Interface, namespace string) {
	const crName = "default"
	err := dyn.Resource(cbtConfigGVR).Namespace(namespace).Delete(ctx, crName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		framework.Logf("deleteCBTConfig %s/%s: %v (ignored)", namespace, crName, err)
	}
}

// waitForCBTConfigStatusEnabled blocks until the namespace's CBTConfig CR has
// status.enabled equal to the expected value, or fails the spec on timeout.
func waitForCBTConfigStatusEnabled(
	ctx context.Context,
	dyn dynamic.Interface,
	namespace string,
	expected bool,
) {
	const crName = "default"
	err := wait.PollUntilContextTimeout(ctx, cbtPollInterval, cbtConvergeTimeout, true,
		func(ctx context.Context) (bool, error) {
			cr, err := dyn.Resource(cbtConfigGVR).Namespace(namespace).Get(ctx, crName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			got, found, err := unstructured.NestedBool(cr.Object, "status", "enabled")
			if err != nil || !found {
				return false, nil
			}
			return got == expected, nil
		})
	framework.ExpectNoError(err,
		"timed out waiting for CBTConfig %s/%s status.enabled to be %t",
		namespace, crName, expected)
}

// getVolumeCBTEnabled returns the FCD's `ChangedBlockTrackingEnabled` flag by
// retrieving the VStorageObject through VSLM. This is the authoritative
// source of truth — `CnsQueryVolume` does NOT carry this flag.
//
// A volume that has never had the flag set returns `false` (vSphere default).
func getVolumeCBTEnabled(ctx context.Context, vs *vSphere, fcdID string) (bool, error) {
	connect(ctx, vs)
	vslmClient, err := vslm.NewClient(ctx, vs.Client.Client)
	if err != nil {
		return false, fmt.Errorf("failed to create VSLM client: %w", err)
	}
	gom := vslm.NewGlobalObjectManager(vslmClient)
	vso, err := gom.Retrieve(ctx, types.ID{Id: fcdID})
	if err != nil {
		return false, fmt.Errorf("failed to retrieve VStorageObject %s: %w", fcdID, err)
	}
	if vso == nil {
		return false, fmt.Errorf("VSLM returned nil VStorageObject for FCD %s", fcdID)
	}
	if vso.Config.ChangedBlockTrackingEnabled == nil {
		return false, nil
	}
	return *vso.Config.ChangedBlockTrackingEnabled, nil
}

// verifyVolumeCBTEnabledWithWait polls getVolumeCBTEnabled until the flag
// matches `expected`, or fails the spec on timeout. Use this whenever the
// test has just triggered an action that should drive the flag (PVC create,
// attach, etc.).
func verifyVolumeCBTEnabledWithWait(ctx context.Context, vs *vSphere, fcdID string, expected bool) {
	var lastSeen bool
	err := wait.PollUntilContextTimeout(ctx, cbtPollInterval, cbtConvergeTimeout, true,
		func(ctx context.Context) (bool, error) {
			got, err := getVolumeCBTEnabled(ctx, vs, fcdID)
			if err != nil {
				return false, err
			}
			lastSeen = got
			return got == expected, nil
		})
	if err != nil {
		framework.Failf("CBT flag for volume %s did not converge to %t (last seen %t): %v",
			fcdID, expected, lastSeen, err)
	}
	ginkgo.By(fmt.Sprintf("Verified CBT flag is %t on volume %s", expected, fcdID))
}

// verifyVolumeCBTFlagSteady asserts the FCD CBT flag stays at `expected` for
// cbtFlagSteadyWindow. Used in the "no CBTConfig CR" case to prove the CSI
// driver did not alter the default. Calls Fail on the first divergence.
func verifyVolumeCBTFlagSteady(ctx context.Context, vs *vSphere, fcdID string, expected bool) {
	deadline := time.Now().Add(cbtFlagSteadyWindow)
	for time.Now().Before(deadline) {
		got, err := getVolumeCBTEnabled(ctx, vs, fcdID)
		framework.ExpectNoError(err,
			"failed to read CBT flag for volume %s during steady-state check", fcdID)
		if got != expected {
			framework.Failf(
				"CBT flag for volume %s changed unexpectedly: expected steady %t, observed %t",
				fcdID, expected, got)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(cbtPollInterval):
		}
	}
	ginkgo.By(fmt.Sprintf("Verified CBT flag stayed %t on volume %s for %s",
		expected, fcdID, cbtFlagSteadyWindow))
}
