# Test #3 — CBT flag on PVC attach to a Pod in VKS (Guest Cluster)

Parent design: [`../cbt.md`](../cbt.md).

## What we verify

When the workload lives in a guest cluster (VKS / TKG):

* The PVC is created in the guest namespace; pvCSI provisions a shadow
  PVC in the matching Supervisor namespace.
* `CBTConfig` lives only on the Supervisor side, and the attach in the
  Supervisor (via `cnsnodevmattachment` / `cnsnodevmbatchattachment`)
  is what re-evaluates and pushes the CBT flag to the FCD.
* From the test's POV: toggling Supervisor `CBTConfig` between PVC
  create and Pod create still drives the FCD CBT flag, observed via
  the same `RetrieveVStorageObject` helper.

## Configuration matrix

| Sub-case | CR at PVC create | CR change before Pod create | Expected CBT flag after attach |
| --- | --- | --- | --- |
| 3a `off→on` | `enabled: false` | toggle to `enabled: true` | `true` |
| 3b `on→off` | `enabled: true` | toggle to `enabled: false` | `false` |

## Preconditions (both must hold; else `Skip()`)

* Cluster flavor: Guest (VKS / TKG).
* **Guest pvCSI FSS:** `isCBTBackupAPIFssEnabledOnGuest(ctx, guestClient)`
  returns `true` (wraps `isCsiFssEnabled(..., common.CSI_Backup_API_FSS)`
  on the guest CSI namespace).
* **Supervisor CRD:** `isCBTConfigCRDInstalledOnSupervisor(ctx, svcRestCfg)`
  returns `true`. The CR is namespace-scoped on the Supervisor and is
  installed by the DP operator independently of any FSS toggle, so this
  check is mandatory even though the workload runs in the guest. A
  testbed where the DP operator is absent will silently skip — that is
  intentional, since there is no way to drive the CR in the first place.

Both gates are required: a CRD-only check on the Supervisor does not
imply guest pvCSI was upgraded with the FSS on, and FSS on alone does
not imply the CRD exists on the Supervisor.

## Setup

* `guestClusterRestConfig = getRestConfigClientForGuestCluster(...)`
  for the guest-side k8s client.
* `svcClient, svcNamespace := getSvcClientAndNamespace()` for the
  Supervisor side. `setCBTConfig` operates against `svcNamespace`.
* Map guest PVC → Supervisor volume handle (FCD ID) using existing
  patterns from `gc_cns_nodevm_attachment.go` /
  `tkgs_ha_utils.go` (`getVolumeIDFromSupervisorCluster` if available;
  otherwise read the bound PV's `Spec.CSI.VolumeHandle` and translate
  via the supervisor PVC label).

## Steps (per sub-case)

1. `setCBTConfig(ctx, svcDyn, svcNamespace, initial)`; wait for
   status reflect.
2. Create a guest PVC bound on the guest storage class. Wait for
   Bound.
3. Resolve `volHandle` (= Supervisor FCD ID). Sanity-check
   `verifyVolumeCBTEnabledWithWait(..., initial)`.
4. `setCBTConfig(ctx, svcDyn, svcNamespace, target)`; wait for status
   reflect.
5. Create a guest Pod that mounts the PVC (`createPodForFSGroup` or
   the vanilla `fpod.CreatePod` path used in `gc_*` tests).
6. Wait for guest Pod Running and supervisor-side attachment via
   `verifyIsAttachedInSupervisor`.
7. `verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, target)`.

## Cleanup (always runs via `defer`)

1. Delete the guest Pod, wait for guest detach + supervisor detach.
2. Delete the guest PVC and wait for guest PV gone +
   `e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)`.
3. `deleteCBTConfig(ctx, svcDyn, svcNamespace)`.
4. `dumpSvcNsEventsOnTestFailure(svcClient, svcNamespace)`.

## Ginkgo

* `Describe`: `[csi-guest] CBT flag on PVC attach to Pod in VKS`
* `It`:
  * `3a: Verify CBT flag is set on attach when supervisor CBTConfig is toggled off→on`
  * `3b: Verify CBT flag is cleared on attach when supervisor CBTConfig is toggled on→off`
* `Label`: `ginkgo.Label(p0, block, tkg, vc80, "cbt")`

## Notes

* The guest cluster only sees the Supervisor PVC by name; CBT is a
  vSphere/FCD concept and is invisible to the guest. The verification
  is unchanged: we always fetch the FCD via the Supervisor's vCenter
  client.
* If `getVolumeIDFromSupervisorCluster` does not exist with that exact
  name, use the equivalent helper already used by the other `gc_*`
  e2e files (do not invent a new one).
