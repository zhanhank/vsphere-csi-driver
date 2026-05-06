# Test #4 — CBT flag on PVC attach to a VM Service VM

Parent design: [`../cbt.md`](../cbt.md).

## What we verify

VM Service VMs attach PVCs through the
`cnsnodevmbatchattachment` controller (see
`pkg/syncer/cnsoperator/controller/cnsnodevmbatchattachment/cnsnodevmbatchattachment_controller.go`).
That controller calls `SyncVolumeCBTState` before each batch attach.
This test verifies the same "CR-changes-between-PVC-and-attach"
contract as test #2, but exercised via the batch path used by VM
Service.

## Configuration matrix

| Sub-case | CR at PVC create | CR change before VM create | Expected CBT flag after attach |
| --- | --- | --- | --- |
| 4a `off→on` | `enabled: false` | toggle to `enabled: true` | `true` |
| 4b `on→off` | `enabled: true` | toggle to `enabled: false` | `false` |

## Preconditions

* Cluster flavor: Supervisor (WCP), VM Service enabled.
* `CSI_Backup_API` WCP capability is on (implicit — see test #1
  preconditions for the rationale).
* `cbtconfigs.cnsdp.vmware.com/v1alpha1` CRD installed on Supervisor
  by the DP operator and discoverable
  (`isCBTConfigCRDInstalledOnSupervisor`); else `Skip()`.
* `vcVersion >= batchAttachSupportedVCVersion` (else fall back to
  single-attach CRD via existing `isBatchAttachSupported` helper —
  the test code is identical because `verifyIsAttachedInSupervisor`
  already abstracts the choice).
* Env: `CONTENT_LIB_URL`, `CONTENT_LIB_THUMBPRINT`,
  `VMSVC_IMAGE_NAME`, `STORAGE_POLICY_FOR_SHARED_DATASTORES`,
  `SVC_NAMESPACE`, `BUSYBOX_IMAGE`, `VC_ADMIN_PWD`. (Same env block
  used by `snapshot_vmservice_vm.go`.)

## Setup (mirrors `snapshot_vmservice_vm.go`)

* `vcRestSessionId = createVcSession4RestApis(ctx)`.
* `dsRef = getDsMoRefFromURL(ctx, datastoreURL)`.
* `storageProfileId = e2eVSphere.GetSpbmPolicyID(storageClassName)`.
* `contentLibId, _ = createAndOrGetContentlibId4Url(...)`.
* `namespace = createTestWcpNs(...)` (test-owned WCP namespace,
  cleaned up in `AfterEach` via `delTestWcpNs`).
* `vmopC` and `cnsopC` controller-runtime clients.
* `vmi = waitNGetVmiForImageName(ctx, vmopC, VMSVC_IMAGE_NAME)`.

## Steps (per sub-case)

1. `setCBTConfig(ctx, dyn, namespace, initial)`; wait for status
   reflect.
2. Create PVC via `createPVCAndQueryVolumeInCNS`. Capture
   `volHandle`. Sanity-check
   `verifyVolumeCBTEnabledWithWait(..., initial)`.
3. `setCBTConfig(ctx, dyn, namespace, target)`; wait for status
   reflect.
4. `secretName = createBootstrapSecretForVmsvcVms(ctx, client, namespace)`.
5. `vm = createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass,
   []*v1.PersistentVolumeClaim{pvc}, vmi, storageClassName, secretName)`.
6. Wait for `vm.Status.PowerState == PoweredOn` and the volume
   attachment is observed via `verifyIsAttachedInSupervisor`
   (this picks the batch CRD when supported).
7. `verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, target)`.

## Cleanup (always runs via `defer`)

1. Delete VM (`deleteVmServiceVm`); wait for detach via
   `verifyIsDetachedInSupervisor`.
2. Delete the bootstrap Secret.
3. `fpv.DeletePersistentVolumeClaim(...)` +
   `e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)`.
4. `deleteCBTConfig(ctx, dyn, namespace)`.
5. `dumpSvcNsEventsOnTestFailure(client, namespace)` and
   `delTestWcpNs(vcRestSessionId, namespace)` (already done in
   `AfterEach` of vmsvc tests).

## Ginkgo

* `Describe`: `[cf-wcp] [csi-supervisor] CBT flag on PVC attach to VM Service VM`
* `It`:
  * `4a: Verify CBT flag is set on attach when CBTConfig is toggled off→on (vmsvc)`
  * `4b: Verify CBT flag is cleared on attach when CBTConfig is toggled on→off (vmsvc)`
* `Label`: `ginkgo.Label(p0, block, wcp, vmServiceVm, vc80, "cbt")`

## Notes

* The batch attach controller is the production path for VM Service
  VMs; older VC builds use the single-attach controller. Both call
  `SyncVolumeCBTState`, so the test does not need to special-case the
  CRD shape.
* If we later add CBT verification on multi-volume VMs, the
  `verifyVolumeCBTEnabledWithWait` helper should be extended to take a
  slice of volume handles; the controller already syncs them in one
  reconcile via `SyncVolumeCBTState(..., volumeIDs...)`.
