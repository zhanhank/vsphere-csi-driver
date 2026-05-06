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
	"os"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// CBT e2e tests verify that the CSI driver honors the namespace-scoped
// CBTConfig CR by setting / clearing the FCD `enableChangedBlockTracking`
// flag at PVC create (Supervisor only) and at attach time (Supervisor +
// VKS + VM Service VM paths).
//
// Skip semantics (see tests/e2e/docs/cbt.md and the e2e-cbt-tests cursor
// rule): all tests skip when the CBTConfig CRD is not installed on the
// Supervisor (the CRD is owned by the DP operator); the VKS test
// additionally skips when the guest pvCSI `CSI_Backup_API` FSS is off.
//
// All four ginkgo.Describe blocks below tag the spec with the literal
// label "cbt" so a single GINKGO_FOCUS=cbt focuses the whole suite.

// --- Test 1: PVC creation on Supervisor -------------------------------------

var _ = ginkgo.Describe("[csi-supervisor] [cf-wcp] CBT flag on PVC create", func() {
	f := framework.NewDefaultFramework("cbt-create")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		storageclass      *storagev1.StorageClass
		svcDyn            dynamic.Interface
		svcRestCfg        *restclient.Config
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()

		if !supervisorCluster {
			ginkgo.Skip("CBT PVC-create test runs only on Supervisor cluster flavor")
		}

		svcDyn, svcRestCfg = newSupervisorDynamicClient()
		if !isCBTConfigCRDInstalledOnSupervisor(ctx, svcRestCfg) {
			ginkgo.Skip("CBTConfig CRD is not installed on the Supervisor (DP operator absent); skipping")
		}

		namespace = getNamespaceToRunTests(f)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		var err error
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"storage class %q must exist in supervisor", storagePolicyName)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Best-effort cleanup; ignore NotFound so we never mask the
		// original failure with cleanup errors.
		deleteCBTConfig(ctx, svcDyn, namespace)
		dumpSvcNsEventsOnTestFailure(client, namespace)
	})

	// runCreateAndVerify provisions a PVC, waits for Bound, and asserts the
	// FCD CBT flag for the supplied configuration. It registers its own
	// PVC cleanup. expectedSteady controls whether we additionally assert
	// the flag stays at the expected value (used in the no-CR case where
	// we want to prove the driver did not touch the default).
	runCreateAndVerify := func(ctx context.Context, expected, expectSteady bool) {
		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace,
			map[string]string{"app": "cbt"}, "", diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty(), "supervisor PV must have a CSI volume handle")

		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, expected)
		if expectSteady {
			verifyVolumeCBTFlagSteady(ctx, &e2eVSphere, volHandle, expected)
		}
	}

	ginkgo.It("should leave CBT flag at default-off when no CBTConfig CR exists [cbt]",
		ginkgo.Label(p0, block, wcp, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			deleteCBTConfig(ctx, svcDyn, namespace)
			runCreateAndVerify(ctx, false, true)
		})

	ginkgo.It("should set CBT flag to true when CBTConfig.spec.enabled=true [cbt]",
		ginkgo.Label(p0, block, wcp, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			setCBTConfig(ctx, svcDyn, namespace, true)
			runCreateAndVerify(ctx, true, false)
		})

	ginkgo.It("should clear CBT flag when CBTConfig.spec.enabled=false [cbt]",
		ginkgo.Label(p0, block, wcp, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			setCBTConfig(ctx, svcDyn, namespace, false)
			runCreateAndVerify(ctx, false, false)
		})
})

// --- Test 2: PVC attach to vSphere Pod (Supervisor) ------------------------

var _ = ginkgo.Describe("[csi-supervisor] [cf-wcp] CBT flag on PVC attach to vSphere Pod", func() {
	f := framework.NewDefaultFramework("cbt-attach-pod")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		client            clientset.Interface
		namespace         string
		storagePolicyName string
		storageclass      *storagev1.StorageClass
		svcDyn            dynamic.Interface
		svcRestCfg        *restclient.Config
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()

		if !supervisorCluster {
			ginkgo.Skip("CBT vSphere-Pod attach test runs only on Supervisor cluster flavor")
		}

		svcDyn, svcRestCfg = newSupervisorDynamicClient()
		if !isCBTConfigCRDInstalledOnSupervisor(ctx, svcRestCfg) {
			ginkgo.Skip("CBTConfig CRD is not installed on the Supervisor (DP operator absent); skipping")
		}

		namespace = getNamespaceToRunTests(f)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		var err error
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		deleteCBTConfig(ctx, svcDyn, namespace)
		dumpSvcNsEventsOnTestFailure(client, namespace)
	})

	// runAttachToggle exercises the contract that the attach controller
	// (cnsnodevmattachment / cnsnodevmbatchattachment) re-syncs the CBT
	// flag from the latest CBTConfig at attach time, even if the CR
	// changed after PVC creation.
	runAttachToggle := func(ctx context.Context, initial, target bool) {
		setCBTConfig(ctx, svcDyn, namespace, initial)

		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace,
			map[string]string{"app": "cbt"}, "", diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		// Sanity: at create time CBT should have followed the initial CR
		// state so we have a known starting point before the toggle.
		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, initial)

		setCBTConfig(ctx, svcDyn, namespace, target)

		pod, err := createPodForFSGroup(ctx, client, namespace, nil,
			[]*v1.PersistentVolumeClaim{pvc}, false, execCommand, nil, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		// Confirm the attach actually completed before asserting the flag;
		// otherwise SyncVolumeCBTState may not have run yet.
		vmUUID, exists := pod.Annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(),
			"vSphere Pod must carry %s annotation", vmUUIDLabel)
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
			"volume %s expected to be attached to PodVM %s", volHandle, vmUUID)

		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, target)
	}

	ginkgo.It("should set CBT flag at attach when CBTConfig is toggled off->on [cbt]",
		ginkgo.Label(p0, block, wcp, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runAttachToggle(ctx, false, true)
		})

	ginkgo.It("should clear CBT flag at attach when CBTConfig is toggled on->off [cbt]",
		ginkgo.Label(p0, block, wcp, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runAttachToggle(ctx, true, false)
		})
})

// --- Test 3: PVC attach to Pod in VKS (guest cluster) ----------------------

var _ = ginkgo.Describe("[csi-guest] CBT flag on PVC attach to Pod in VKS", func() {
	f := framework.NewDefaultFramework("cbt-attach-vks")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		guestClient       clientset.Interface
		guestNamespace    string
		storagePolicyName string
		storageclass      *storagev1.StorageClass
		svcDyn            dynamic.Interface
		svcRestCfg        *restclient.Config
		svcClient         clientset.Interface
		svcNamespace      string
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		guestClient = f.ClientSet
		bootstrap()

		if !guestCluster {
			ginkgo.Skip("CBT VKS attach test runs only on guest cluster flavor")
		}

		// Both gates must hold for the test to be meaningful: pvCSI FSS on
		// the guest AND the CBTConfig CRD on the supervisor.
		if !isCBTBackupAPIFssEnabledOnGuest(ctx, guestClient) {
			ginkgo.Skip("guest pvCSI CSI_Backup_API FSS is off; skipping")
		}
		svcDyn, svcRestCfg = newSupervisorDynamicClient()
		if !isCBTConfigCRDInstalledOnSupervisor(ctx, svcRestCfg) {
			ginkgo.Skip("CBTConfig CRD is not installed on the Supervisor (DP operator absent); skipping")
		}

		guestNamespace = f.Namespace.Name
		svcClient, svcNamespace = getSvcClientAndNamespace()
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		var err error
		storageclass, err = guestClient.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"guest storage class %q must exist", storagePolicyName)
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// CR lives on the supervisor namespace, not the guest namespace.
		deleteCBTConfig(ctx, svcDyn, svcNamespace)
		dumpSvcNsEventsOnTestFailure(svcClient, svcNamespace)
	})

	runVKSAttachToggle := func(ctx context.Context, initial, target bool) {
		setCBTConfig(ctx, svcDyn, svcNamespace, initial)

		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, guestClient, guestNamespace,
			map[string]string{"app": "cbt"}, "", diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// In guest flavor, pv.Spec.CSI.VolumeHandle is the supervisor PVC
		// name; translate to the FCD ID so we can talk to VSLM.
		svcPVCName := pvs[0].Spec.CSI.VolumeHandle
		volHandle := getVolumeIDFromSupervisorCluster(svcPVCName)
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty(),
			"failed to resolve supervisor FCD ID for guest PV %s", pvs[0].Name)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := fpv.DeletePersistentVolumeClaim(ctx, guestClient, pvc.Name, guestNamespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, initial)

		setCBTConfig(ctx, svcDyn, svcNamespace, target)

		pod, err := createPodForFSGroup(ctx, guestClient, guestNamespace, nil,
			[]*v1.PersistentVolumeClaim{pvc}, false, execCommand, nil, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := fpod.DeletePodWithWait(ctx, guestClient, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		// In guest flavor the pod node name maps to the supervisor worker
		// VM UUID via getVMUUIDFromNodeName.
		vmUUID, err := getVMUUIDFromNodeName(pod.Spec.NodeName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(guestClient, volHandle, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(isDiskAttached).To(gomega.BeTrue(),
			"volume %s expected to be attached to worker VM %s", volHandle, vmUUID)

		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, target)
	}

	ginkgo.It("should set CBT flag at attach when supervisor CBTConfig is toggled off->on [cbt]",
		ginkgo.Label(p0, block, tkg, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runVKSAttachToggle(ctx, false, true)
		})

	ginkgo.It("should clear CBT flag at attach when supervisor CBTConfig is toggled on->off [cbt]",
		ginkgo.Label(p0, block, tkg, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runVKSAttachToggle(ctx, true, false)
		})
})

// --- Test 4: PVC attach to VM Service VM -----------------------------------

var _ = ginkgo.Describe("[cf-wcp] [csi-supervisor] CBT flag on PVC attach to VM Service VM", func() {
	f := framework.NewDefaultFramework("cbt-attach-vmsvc")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	f.SkipNamespaceCreation = true // we mint our own WCP namespace

	var (
		client           clientset.Interface
		namespace        string
		datastoreURL     string
		storageClassName string
		storageProfileId string
		vcRestSessionId  string
		vmi              string
		vmClass          string
		vmopC            ctlrclient.Client
		dsRef            types.ManagedObjectReference
		svcDyn           dynamic.Interface
		svcRestCfg       *restclient.Config
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()

		if !supervisorCluster {
			ginkgo.Skip("CBT VM Service VM attach test runs only on Supervisor cluster flavor")
		}

		svcDyn, svcRestCfg = newSupervisorDynamicClient()
		if !isCBTConfigCRDInstalledOnSupervisor(ctx, svcRestCfg) {
			ginkgo.Skip("CBTConfig CRD is not installed on the Supervisor (DP operator absent); skipping")
		}

		storageClassName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef = getDsMoRefFromURL(ctx, datastoreURL)
		storageProfileId = e2eVSphere.GetSpbmPolicyID(storageClassName)

		vcRestSessionId = createVcSession4RestApis(ctx)
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId,
			GetAndExpectStringEnvVar(envContentLibraryUrl), dsRef.Value, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		framework.Logf("Creating WCP namespace for the CBT VM Service test")
		namespace = createTestWcpNs(
			vcRestSessionId, storageProfileId, vmClass, contentLibId, getSvcId(vcRestSessionId, &e2eVSphere))

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		vmi = waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		deleteCBTConfig(ctx, svcDyn, namespace)
		dumpSvcNsEventsOnTestFailure(client, namespace)
		delTestWcpNs(vcRestSessionId, namespace)
		gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())
	})

	runVmsvcAttachToggle := func(ctx context.Context, initial, target bool) {
		setCBTConfig(ctx, svcDyn, namespace, initial)

		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc, pvs, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace,
			map[string]string{"app": "cbt"}, "", diskSize, storageclass, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		volHandle := pvs[0].Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		})

		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, initial)

		setCBTConfig(ctx, svcDyn, namespace, target)

		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			_ = client.CoreV1().Secrets(namespace).Delete(ctx, secretName, *metav1.NewDeleteOptions(0))
		})

		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			deleteVmServiceVm(ctx, vmopC, namespace, vm.Name)
		})

		// Wait for the VM to be powered on and report the volume in its
		// status. This is a stricter signal than just "VM Created" and
		// matches the production attach-completion checkpoint that runs
		// SyncVolumeCBTState in the batch attach controller.
		gomega.Expect(waitForVmsvcVolumeAttached(ctx, vmopC, vm, pvc.Name)).To(gomega.Succeed())

		verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, target)
	}

	ginkgo.It("should set CBT flag at attach when CBTConfig is toggled off->on (vmsvc) [cbt]",
		ginkgo.Label(p0, block, wcp, vmServiceVm, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runVmsvcAttachToggle(ctx, false, true)
		})

	ginkgo.It("should clear CBT flag at attach when CBTConfig is toggled on->off (vmsvc) [cbt]",
		ginkgo.Label(p0, block, wcp, vmServiceVm, vc80, "cbt"), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runVmsvcAttachToggle(ctx, true, false)
		})
})

// waitForVmsvcVolumeAttached waits until the VirtualMachine status reports
// the named PVC volume as attached. Returns an error on timeout instead of
// failing the spec so callers can wrap with their own context.
//
// We do not reuse waitNverifyPvcsAreAttachedToVmsvcVm because that helper
// also depends on cnsopC + the batch-attach CR shape; the FCD-side CBT
// check below only needs the VM .Status.Volumes signal.
func waitForVmsvcVolumeAttached(
	ctx context.Context,
	vmopC ctlrclient.Client,
	vm *vmopv1.VirtualMachine,
	pvcName string,
) error {
	return wait.PollUntilContextTimeout(ctx, poll*5, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			cur, err := getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name)
			if err != nil {
				return false, err
			}
			if cur.Status.PowerState != vmopv1.VirtualMachinePoweredOn {
				return false, nil
			}
			for _, vol := range cur.Status.Volumes {
				if vol.Name == pvcName && vol.Attached {
					return true, nil
				}
			}
			return false, nil
		})
}
