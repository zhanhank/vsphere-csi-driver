# Test #1 — CBT flag on PVC creation in Supervisor

Parent design: [`../cbt.md`](../cbt.md).

## What we verify

The supervisor `CreateVolume` path (`pkg/csi/service/wcp/controller.go`)
honors the namespace's current `CBTConfig` CR state when provisioning a
new PVC. Specifically that the FCD's
`Config.BaseConfigInfo.ChangedBlockTrackingEnabled` flag matches the
following matrix immediately after the PVC reaches `Bound`:

| `CBTConfig` CR state | Expected FCD CBT flag after Bound |
| --- | --- |
| Absent (no CR in namespace) | `false` (vSphere default; driver did not touch it) |
| `spec.enabled: true` (and `Status.Enabled == true`) | `true` |
| `spec.enabled: false` (and `Status.Enabled == false`) | `false` |

## Preconditions

* Cluster flavor: Supervisor (WCP).
* `CSI_Backup_API` WCP capability is on (no probe — implicit; the test
  would fail loudly if off because the controller would never call
  `SetVolumeControlFlags`).
* `cbtconfigs.cnsdp.vmware.com/v1alpha1` CRD installed on Supervisor by
  the DP operator and discoverable
  (`isCBTConfigCRDInstalledOnSupervisor` returns `true`); else
  `Skip()`. The supervisor does **not** install this CRD as a
  side-effect of the FSS being on.
* A storage class for shared datastore
  (`STORAGE_POLICY_FOR_SHARED_DATASTORES`) exists in the test
  supervisor namespace.

## Test variants

Implemented as a Ginkgo `DescribeTable`-style structure or three
sequential `It` blocks, one per CR state row above.

## Steps (per variant)

1. Pick the namespace (`getSvcClientAndNamespace` returns a per-test
   supervisor namespace; tests #2/#4 use `createTestWcpNs` instead, this
   test reuses the standard one).
2. Apply CR state for this variant:
   * **No CR**: `deleteCBTConfig(ctx, dyn, ns)` (idempotent best-effort).
   * **Enabled**: `setCBTConfig(ctx, dyn, ns, true)` then
     `waitForCBTConfigStatusEnabled(ctx, dyn, ns, true)`.
   * **Disabled**: `setCBTConfig(ctx, dyn, ns, false)` then
     `waitForCBTConfigStatusEnabled(ctx, dyn, ns, false)`.
3. Create a 1Gi block PVC via `createPVCAndQueryVolumeInCNS`. Capture
   `volHandle = pvs[0].Spec.CSI.VolumeHandle` (= FCD ID on Supervisor).
4. `verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, expected)`
   where `expected` is taken from the matrix above.

## Cleanup (always runs via `defer`)

1. `fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, ns)` and
   `e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)`.
2. `deleteCBTConfig(ctx, dyn, ns)` (`IsNotFound` ignored).
3. `dumpSvcNsEventsOnTestFailure(client, ns)`.

The test must NOT mutate any other CRs in the namespace.

## Ginkgo

* `Describe`: `[csi-supervisor] [cf-wcp] CBT flag on PVC create`
* `It` examples:
  * `Verify CBT flag remains default-off when no CBTConfig CR exists`
  * `Verify CBT flag is enabled when CBTConfig.spec.enabled=true`
  * `Verify CBT flag is disabled when CBTConfig.spec.enabled=false`
* `Label`: `ginkgo.Label(p0, block, wcp, vc80, "cbt")`

## Notes

* "No CR" assertion uses `verifyVolumeCBTEnabledWithWait(..., false)` —
  the controller short-circuits on `configured == false`, so the flag
  stays at vSphere default. We assert "still false after a short poll"
  rather than "never changed" to avoid flakes due to background reconcile
  windows.
* Static-provisioning (existing FCD bound to a new PV) is intentionally
  out of scope here; the controller does not run the CBT sync on the
  static path.
