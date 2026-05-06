# Volume Changed Block Tracking (CBT) — E2E Test Design

This document is the umbrella design for the e2e tests that verify
namespace-scoped Volume CBT (Changed Block Tracking) enablement on the
vSphere CSI driver. Per-test-case designs live under
[`./cbt/`](./cbt/).

## 1. Background

Volume CBT is enabled on a per-volume basis by setting the FCD control flag
`enableChangedBlockTracking` (govmomi
`cnstypes.CnsVolumeControlFlagsEnableChangedBlockTracking`).
The driver decides whether to set/clear that flag using a namespace-scoped
singleton CR `CBTConfig` (`cbt.storage.k8s.io` group, package
`pkg/apis/cbtconfig/v1alpha1`, GVR
`cbtconfigs.cnsdp.vmware.com/v1alpha1`).

The controller-side decision lives in
`pkg/csi/service/common/util.go::CBTStateForNamespace` /
`SyncVolumeCBTState`. It returns three states:

| `CBTConfig` CR (in PVC namespace) | `Status.Enabled` | Driver action |
| --- | --- | --- |
| Absent | n/a | Skip — leave the volume's CBT flag untouched (vSphere default = off). |
| Present | `nil` (not yet reconciled) | Skip — wait for next reconcile. |
| Present | `true` | `SetVolumeControlFlags(enableChangedBlockTracking)` |
| Present | `false` | `ClearVolumeControlFlags(enableChangedBlockTracking)` |

CBT enablement is invoked at two boundaries:

1. **PVC create** (Supervisor only) — `pkg/csi/service/wcp/controller.go::CreateVolume`
   calls `SetVolumeCbtFlagsUtil` after volume create when CBT is requested.
2. **Volume attach** — `cnsnodevmattachment` (single) and
   `cnsnodevmbatchattachment` (batch) controllers call
   `SyncVolumeCBTState` before issuing the CNS attach so the latest
   namespace intent is applied at attach time.
   This is the contract that lets the tests change `CBTConfig` *between*
   PVC creation and pod/VM creation and still see the new value reflected
   on the volume.

Feature gating has two independent toggles, **both** must be on for the
end-to-end CBT path to function:

1. **`CSI_Backup_API` capability/FSS** — gates the controller code
   (`pkg/csi/service/common/constants.go`).
   * Supervisor (WCP): the WCP capability `CSI_Backup_API`.
   * Guest (VKS, pvCSI): the CSI internal-features-states ConfigMap key
     `CSI_Backup_API` (mapped to the same WCP capability via
     `WCPFeatureAssociatedWithPVCSI`).
2. **`CBTConfig` CRD installation** — installed by an external Data
   Protection (DP) operator on the **Supervisor**. The CRD is **not**
   installed by the supervisor itself as a side-effect of the
   `CSI_Backup_API` capability being on. Without the CRD, the
   namespace-scoped CBT decision path is inert (no CR can ever exist) on
   both Supervisor and VKS workloads (the VKS path resolves the CR on the
   Supervisor side).

## 2. Scope and out-of-scope

### In scope

* Verify the `enableChangedBlockTracking` control flag transitions on the
  vSphere FCD as a function of the `CBTConfig` CR state across the four
  scenarios listed below.
* Run on three CBT configurations:
  1. No `CBTConfig` CR in the namespace.
  2. `CBTConfig{spec.enabled: true}`.
  3. `CBTConfig{spec.enabled: false}`.
* Cover three workload flavors: vSphere Pod (Supervisor), Pod in VKS
  (guest cluster), VM Service VM (Supervisor).

### Out of scope (deliberate)

* CSI `SnapshotMetadata` RPC behavior (`GetMetadataAllocated` /
  `GetMetadataDelta`). Verifying the gRPC streams requires an
  `external-snapshot-metadata` sidecar deployment plus VADP-like
  consumers; covered by separate snapshot-metadata e2e work.
* Backup/restore data correctness.
* CBTConfig admission/validation (covered by unit tests under
  `pkg/syncer/cbtoperator/...` and the admission package).

## 3. Test-case index

| # | Doc | Workload | What it asserts |
| --- | --- | --- | --- |
| 1 | [test1_pvc_creation_supervisor.md](./cbt/test1_pvc_creation_supervisor.md) | Supervisor PVC only | CBT flag at create time follows current CR state |
| 2 | [test2_pvc_attach_vsphere_pod.md](./cbt/test2_pvc_attach_vsphere_pod.md) | vSphere Pod (Supervisor) | CBT flag re-evaluated on attach, even when CR changed after PVC create |
| 3 | [test3_pvc_attach_vks.md](./cbt/test3_pvc_attach_vks.md) | Pod in VKS (guest) | Same as #2 but PVC + Pod live in guest; supervisor CR drives flag |
| 4 | [test4_pvc_attach_vmservice_vm.md](./cbt/test4_pvc_attach_vmservice_vm.md) | VM Service VM (Supervisor) | Same as #2 but attach is via VM Service (`cnsnodevmbatchattachment`) |

Each per-test doc spells out: configuration matrix, steps, assertions,
cleanup, ginkgo labels, env vars, and the new helpers it consumes.

## 4. Skip/gate matrix (requirements #1 and #2)

Every CBT `It` block guards on **both** toggles described in §1. The
`CBTConfig` CRD lives only on the Supervisor, so the CRD discovery is
always against the Supervisor kubeconfig — including the VKS test, which
needs to drive the CR from the Supervisor side.

```text
if supervisor flavor:
    # FSS check is implicit: no helper exists for the WCP capability,
    # but the test would fail loudly (not silently) if the capability is
    # off because SetVolumeControlFlags would never be called.
    skip unless CBTConfig CRD is discoverable on Supervisor
                (cbtconfigs.cnsdp.vmware.com/v1alpha1)

if guest flavor:
    skip unless isCsiFssEnabled(ctx, guestClient, csiSystemNamespace,
                                common.CSI_Backup_API_FSS)
    AND  skip unless CBTConfig CRD is discoverable on Supervisor
                     (cbtconfigs.cnsdp.vmware.com/v1alpha1)
```

Rationale:

* The CRD is installed by the DP operator independently of the
  `CSI_Backup_API` toggle. Both must be present for the test to be
  meaningful, so we check the CRD explicitly and rely on a loud failure
  if the capability is off (rather than a silent skip that would mask
  CBT regressions on configured testbeds).
* For VKS, the `CBTConfig` CR is namespace-scoped on the Supervisor;
  the guest pvCSI never sees it. So the VKS test still needs the CRD
  on the Supervisor to manipulate the CR there.

If we later need a positive WCP-capability probe on Supervisor (e.g. to
distinguish "capability off" from "DP operator missing"), we'll add it
as a follow-up; CRD discovery is sufficient for the current test set.

## 5. New e2e helpers (proposed `tests/e2e/cbt_utils.go`)

All helpers use existing patterns; nothing in the framework needs to
change.

| Helper | Purpose | Notes |
| --- | --- | --- |
| `isCBTConfigCRDInstalledOnSupervisor(ctx, svcRestCfg)` | Returns `true` when GVR `cbtconfigs.cnsdp.vmware.com/v1alpha1` is present in discovery on the Supervisor. | Used by Skip() in **all** CBT tests (Supervisor + VKS). The CRD is owned by the DP operator. Tolerates `IsNoMatchError`. |
| `isCBTBackupAPIFssEnabledOnGuest(ctx, guestClient)` | Wraps `isCsiFssEnabled(ctx, guestClient, csiSystemNamespace, common.CSI_Backup_API_FSS)`. | Used by the VKS test only (in addition to the Supervisor CRD check). |
| `setCBTConfig(ctx, dyn, ns, enabled)` | Create-or-update the namespace's singleton `CBTConfig` CR with `spec.enabled=enabled`. | Idempotent. Returns the CR. |
| `deleteCBTConfig(ctx, dyn, ns)` | Best-effort delete; `IsNotFound` is fine. | Used in `defer` for cleanup. |
| `waitForCBTConfigStatusEnabled(ctx, dyn, ns, expected)` | Poll until `Status.Enabled != nil && *Status.Enabled == expected`. | Times out at `pollTimeout`. |
| `getVolumeCBTEnabled(ctx, vs, fcdID)` | Calls `RetrieveVStorageObject(fcdID)` and returns `Config.BaseConfigInfo.ChangedBlockTrackingEnabled` (defaults to `false` when nil). | Reuses `vs.Client.Client.ServiceContent.VStorageObjectManager` (already used in `tests/e2e/vsphere.go::createFCD`). |
| `verifyVolumeCBTEnabledWithWait(ctx, vs, fcdID, expected)` | Polls `getVolumeCBTEnabled` until match or timeout. | Required because `SetVolumeControlFlags` is async wrt PVC Bound / attach. |

GVR / scheme: identical to the unit-test pattern at
`pkg/csi/service/common/util_test.go:650`:

```text
gvr := cbtconfigv1alpha1.GroupVersion.WithResource(cbtconfigv1alpha1.CBTConfigResource)
```

## 6. Cleanup contract (requirement #4)

Every test sets up its CBTConfig + PVC + workload inside a Ginkgo `It`
block whose `defer`s run regardless of failure. Order of teardown:

1. Delete the workload (Pod / vSphere Pod / VM Service VM).
2. Wait for detach (existing `verifyIsDetachedInSupervisor`).
3. Delete PVC and wait for `e2eVSphere.waitForCNSVolumeToBeDeleted`.
4. Delete the `CBTConfig` CR (`deleteCBTConfig`, ignore NotFound).
5. Delete the test namespace (Supervisor: `delTestWcpNs`; guest: framework default).
6. `dumpSvcNsEventsOnTestFailure(client, namespace)` for diagnostics.

Helpers must tolerate partial state (e.g. PVC already gone, CR already
absent) so the failure path doesn't mask the original error.

## 7. Ginkgo labels and focus

Following existing convention (`vsphere_volume_expansion.go`,
`snapshot_vmservice_vm.go`):

| Test | Labels |
| --- | --- |
| 1 | `[csi-supervisor]` `[cf-wcp]`, `ginkgo.Label(p0, block, wcp, vc80, "cbt")` |
| 2 | `[csi-supervisor]` `[cf-wcp]`, `ginkgo.Label(p0, block, wcp, vc80, "cbt")` |
| 3 | `[csi-guest]`, `ginkgo.Label(p0, block, tkg, vc80, "cbt")` |
| 4 | `[csi-supervisor]` `[cf-wcp]` `vmServiceVm`, `ginkgo.Label(p0, block, wcp, vmServiceVm, vc80, "cbt")` |

Run a focused subset:

```bash
export GINKGO_FOCUS="cbt"
make test-e2e
```

Or focus on a single case via the `It` description, e.g.:

```bash
export GINKGO_FOCUS="Verify CBT flag is enabled at PVC create on Supervisor"
```

## 8. Required environment

Reuses the standard supervisor / guest / vmservice env set already
documented in [`./supervisor_cluster_setup.md`](./supervisor_cluster_setup.md)
and [`./guest_cluster_setup.md`](./guest_cluster_setup.md). No new env
vars are introduced.

Cluster-flavor-specific minimums:

* Tests 1, 2, 4 (Supervisor + vSphere Pod + VM Service VM):
  `KUBECONFIG`, `SUPERVISOR_CLUSTER_KUBE_CONFIG`, `E2E_TEST_CONF_FILE`,
  `STORAGE_POLICY_FOR_SHARED_DATASTORES`, `SVC_NAMESPACE`,
  `SHARED_VSPHERE_DATASTORE_URL`, `BUSYBOX_IMAGE`, `VC_ADMIN_PWD`,
  plus `CONTENT_LIB_URL`, `CONTENT_LIB_THUMBPRINT`, `VMSVC_IMAGE_NAME`
  for test #4.
* Test 3 (VKS / guest):
  `KUBECONFIG` (guest), `SUPERVISOR_CLUSTER_KUBE_CONFIG`,
  `E2E_TEST_CONF_FILE`, `GC_NAMESPACE` (or use the test's default),
  plus the standard supervisor storage policy + datastore exports.

## 9. Framework gaps (and how we close them)

| Gap | Mitigation |
| --- | --- |
| No "WCP capability" probe in `tests/e2e`. | Skip on CBTConfig CRD discovery; CSI_Backup_API capability is checked implicitly (test would fail loudly, not silently, if it is off). Add a probe later only if false-skip noise becomes a problem. |
| `CnsVolume` (CnsQueryVolume result) does not surface the CBT control flag. | New helper `getVolumeCBTEnabled` calls `RetrieveVStorageObject` via the existing `ServiceContent.VStorageObjectManager`. |
| No `CBTConfig` CR helpers in `tests/e2e`. | Add the small helper set in §5. Pattern mirrors `pkg/csi/service/common/util_test.go`. |
| The DP operator that installs the `CBTConfig` CRD is not part of the CSI repo / framework. | The CRD discovery skip handles testbeds where the DP operator is absent — including in the VKS test path, since the CR lives on the Supervisor. |

No structural changes to the e2e framework are required.

## 10. Open follow-ups (not in this PR)

* Verify CBT change-id is actually populated on snapshots
  (`VStorageObject.Snapshot.ChangedBlockTrackingId`) once the snapshot
  metadata RPC tests land.
* Negative test: `CBTConfig` admission rejects a second CR in the same
  namespace (singleton). Probably belongs in the admission package's
  e2e, not here.
