# Test #2 — CBT flag on PVC attach to a vSphere Pod (Supervisor)

Parent design: [`../cbt.md`](../cbt.md).

## What we verify

The attach path on Supervisor (`cnsnodevmattachment` controller) calls
`SyncVolumeCBTState` *before* the CNS attach. So even if the
`CBTConfig` CR was different at PVC create time, by the time the
vSphere Pod has the volume attached, the FCD's CBT flag must reflect
the **current** CR state.

This is the contract that allows backup software to flip CBT on/off
late and have it take effect on the next attach.

## Configuration matrix

Two sub-cases (per requirement #2):

| Sub-case | CR at PVC create | CR change between create and attach | Expected CBT flag after attach |
| --- | --- | --- | --- |
| 2a `off→on` | `enabled: false` | toggle to `enabled: true` | `true` |
| 2b `on→off` | `enabled: true` | toggle to `enabled: false` | `false` |

The "no CR" pre-state from test #1 is **not** repeated here — that
matrix dimension is fully covered by test #1.

## Preconditions

* Cluster flavor: Supervisor (WCP).
* `CSI_Backup_API` WCP capability is on (implicit — see test #1
  preconditions for the rationale).
* `cbtconfigs.cnsdp.vmware.com/v1alpha1` CRD installed on Supervisor
  by the DP operator and discoverable
  (`isCBTConfigCRDInstalledOnSupervisor`); else `Skip()`.
* Storage class for shared datastore exists in the supervisor
  namespace.
* `BUSYBOX_IMAGE` env set (used for the vSphere Pod image).

## Steps (per sub-case)

1. `setCBTConfig(ctx, dyn, ns, initial)`; wait for status reflect.
2. Create PVC via `createPVCAndQueryVolumeInCNS`. Capture
   `volHandle`. Sanity-check (optional):
   `verifyVolumeCBTEnabledWithWait(..., initial)`.
3. `setCBTConfig(ctx, dyn, ns, target)`; wait for status reflect.
4. Create a vSphere Pod that mounts the PVC using the existing
   helper `createPodForFSGroup(ctx, client, ns, nil, []*v1.PVC{pvc},
   false, execCommand, nil, nil)`.
5. Wait for attachment to be observed:
   * Compute expected CRD instance name.
   * `verifyIsAttachedInSupervisor(ctx, f, nodeName, volHandle,
     crdVersion, crdGroup)` — selects single vs batch CRD via
     `isBatchAttachSupported` (already handled by that helper).
6. `verifyVolumeCBTEnabledWithWait(ctx, &e2eVSphere, volHandle, target)`.
7. Optional sanity write (`/data/foo`) into the volume to confirm I/O
   works after CBT toggling. Nothing about the data correctness is
   asserted; this is purely a smoke check.

## Cleanup (always runs via `defer`)

1. Delete the Pod (`fpod.DeletePodWithWait`).
2. `verifyIsDetachedInSupervisor(...)`.
3. `fpv.DeletePersistentVolumeClaim(...)` +
   `e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)`.
4. `deleteCBTConfig(ctx, dyn, ns)`.
5. `dumpSvcNsEventsOnTestFailure(client, ns)`.

## Ginkgo

* `Describe`: `[csi-supervisor] [cf-wcp] CBT flag on PVC attach to vSphere Pod`
* `It`:
  * `2a: Verify CBT flag is set on attach when CBTConfig is toggled off→on`
  * `2b: Verify CBT flag is cleared on attach when CBTConfig is toggled on→off`
* `Label`: `ginkgo.Label(p0, block, wcp, vc80, "cbt")`

## Notes

* The "vSphere Pod" here is a regular `corev1.Pod` created in a
  Supervisor namespace; the supervisor pod runtime executes it as a
  vSphere Pod (CRX/spherelet) — same path as
  `tests/e2e/vsphere_volume_fsgroup.go`. We do not need an explicit
  vSphere-Pod abstraction.
* The CRD-based attach assertion (`verifyIsAttachedInSupervisor`) is
  what distinguishes a real attach from "pod is Running but volume
  hasn't reached CNS yet". Without it the CBT verification can race
  the controller and intermittently see stale flag values.
