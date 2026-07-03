package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LINBIT/golinstor/client"
	"github.com/piraeusdatastore/linstor-csi/pkg/driver"
	csilinstor "github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/operations"
	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

// AffinityReconciler compares existing PVs with LINSTOR Resources.
// If it finds that the PV has affinity settings contradicting the LINSTOR Resources, it will generate an Operation
// that brings them back into sync.
type AffinityReconciler struct {
	PVIndexer        cache.SharedIndexInformer
	NewPVTimeout     time.Duration
	LinstorClient    *hlclient.HighLevelClient
	KubernetesClient kubernetes.Interface
	EventRecorder    events.EventRecorder
}

func (a *AffinityReconciler) Name() string {
	return "AffinityReconciler"
}

func (a *AffinityReconciler) GenerateOperations(ctx context.Context, now time.Time) ([]operations.Operation, error) {
	resources, err := a.LinstorClient.ResourceDefinitions.GetAll(ctx, client.RDGetAllRequest{WithVolumeDefinitions: true})
	if err != nil {
		return nil, fmt.Errorf("failed to list resource definitions: %w", err)
	}

	var ops []operations.Operation
	for i := range resources {
		resource := &resources[i]

		// A single RD can back multiple PVs, one per volume number.
		indexed, _ := a.PVIndexer.GetIndexer().ByIndex("rd", resource.Name)

		pvs := make([]*corev1.PersistentVolume, 0, len(indexed))
		for _, obj := range indexed {
			pvs = append(pvs, obj.(*corev1.PersistentVolume).DeepCopy())
		}

		ops = append(ops, a.generateOperations(ctx, resource, pvs, now)...)
	}

	return ops, nil
}

func (a *AffinityReconciler) generateOperations(ctx context.Context, rd *client.ResourceDefinitionWithVolumeDefinition, pvs []*corev1.PersistentVolume, now time.Time) []operations.Operation {
	if rd.Props[version.SkipPropKey] != "" {
		klog.V(2).Infof("Skipping reconciliation of RD '%s' because of LINSTOR property '%s'", rd.Name, version.SkipPropKey)
		return nil
	}

	// Saved PV state left behind on a volume definition by a replace that did not
	// run to completion, keyed by volume number. Present when a previous replace
	// was interrupted before it could re-create the PV and clean up after itself.
	saved := map[int]string{}
	for _, vd := range rd.VolumeDefinitions {
		if vd.VolumeNumber == nil {
			continue
		}
		if raw, ok := vd.Props[version.SavedPVPropKey]; ok {
			saved[int(*vd.VolumeNumber)] = raw
		}
	}

	// Live PVs, keyed by the volume number encoded in their CSI volume handle.
	live := map[int]*corev1.PersistentVolume{}
	for _, pv := range pvs {
		if pv.Spec.CSI == nil {
			continue
		}

		id, err := volume.ParseVolumeId(pv.Spec.CSI.VolumeHandle)
		if err != nil {
			klog.V(1).ErrorS(err, "Skipping PV with unparseable volume handle", "PV", pv.Name, "handle", pv.Spec.CSI.VolumeHandle)
			continue
		}

		if other, ok := live[id.VolumeNumber]; ok {
			klog.V(1).Infof("PVs '%s' and '%s' both map to volume '%s', skipping", other.Name, pv.Name, id)
			continue
		}

		live[id.VolumeNumber] = pv
	}

	// Reconcile every volume that has a live PV and/or leftover saved state.
	volNrs := make([]int, 0, len(live)+len(saved))
	for volNr := range live {
		volNrs = append(volNrs, volNr)
	}
	for volNr := range saved {
		if _, ok := live[volNr]; !ok {
			volNrs = append(volNrs, volNr)
		}
	}
	sort.Ints(volNrs)

	var ops []operations.Operation
	for _, volNr := range volNrs {
		savedRaw, hasSaved := saved[volNr]

		o, err := a.generateVolumeOperation(ctx, volume.ID{ResourceName: rd.Name, VolumeNumber: volNr}, live[volNr], savedRaw, hasSaved, now)
		if err != nil {
			klog.V(1).ErrorS(err, "Skipping reconciliation because of error", "RD", rd.Name, "volume", volNr)
			continue
		}

		if o != nil {
			ops = append(ops, *o)
		}
	}

	return ops
}

func (a *AffinityReconciler) generateVolumeOperation(ctx context.Context, id volume.ID, pv *corev1.PersistentVolume, savedRaw string, hasSavedProp bool, now time.Time) (*operations.Operation, error) {
	needsApply := false

	klog.V(2).Infof("Ensure that we have a matching PV to update for Volume %s", id)
	if pv == nil {
		if !hasSavedProp {
			klog.V(1).Infof("Volume %s has no matching PV and no saved PV configuration, nothing to do", id)
			return nil, nil
		}
		klog.V(2).Infof("Decoding saved PV from property")

		// NB: we deserialize a PersistentVolumeApplyConfiguration here as PersistentVolume. This is fine, because
		// the ApplyConfiguration is just missing some optional fields, otherwise they are the same.
		pv = &corev1.PersistentVolume{}
		err := json.Unmarshal([]byte(savedRaw), pv)
		if err != nil {
			return nil, fmt.Errorf("failed to parse saved PV resource: %w", err)
		}

		needsApply = true
	} else if hasSavedProp {
		// NB: we deserialize a PersistentVolumeApplyConfiguration here as PersistentVolume. This is fine, because
		// the ApplyConfiguration is just missing some optional fields, otherwise they are the same.
		savedPV := &corev1.PersistentVolume{}
		err := json.Unmarshal([]byte(savedRaw), savedPV)
		if err != nil {
			return nil, fmt.Errorf("failed to parse saved PV resource: %w", err)
		}

		if pv.DeletionTimestamp != nil || pv.Spec.PersistentVolumeReclaimPolicy != savedPV.Spec.PersistentVolumeReclaimPolicy {
			klog.V(2).Infof("decoded PV does not match current PV or current PV is being deleted, need to re-apply")
			pv.Spec.PersistentVolumeReclaimPolicy = savedPV.Spec.PersistentVolumeReclaimPolicy
			needsApply = true
		} else {
			klog.V(2).Infof("Need to remove stale PV property from volume '%s'", id)
			return operations.RemovePVProperty(a.LinstorClient, id), nil
		}
	}

	if pv.Annotations[version.SkipAnnotation] != "" {
		klog.V(2).Infof("Skipping reconciliation of PV '%s' because of annotation '%s'", pv.Name, version.SkipAnnotation)
		return nil, nil
	}

	if pv.Spec.CSI == nil {
		klog.V(2).Infof("PV '%s' not provisioned by CSI, skipping", pv.Name)
		return nil, nil
	}

	if pv.Spec.CSI.Driver != csilinstor.DriverName {
		klog.V(2).Infof("PV '%s' not provisioned by LINSTOR CSI, skipping", pv.Name)
		return nil, nil
	}

	if pv.Status.Phase == corev1.VolumeReleased {
		klog.V(2).Infof("PV '%s' is in phase '%s', skipping", pv.Name, corev1.VolumeReleased)
		return nil, nil
	}

	if pv.CreationTimestamp.Add(a.NewPVTimeout).After(now) {
		klog.V(2).Infof("PV '%s' was created at %s, which is within the initial grace period, skipping", pv.Name, pv.CreationTimestamp)
		return nil, nil
	}

	klog.V(2).Infof("Check if LINSTOR API Cache for Resource '%s' has resource available", id.ResourceName)
	ress, err := a.LinstorClient.Resources.GetAll(ctx, id.ResourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get resources for resource definition '%s': %w", id.ResourceName, err)
	}

	if len(ress) == 0 {
		klog.V(2).Infof("RD '%s' not (yet) present in cache, skipping", id.ResourceName)
		return nil, nil
	}

	klog.V(2).Infof("Determine access policy to apply to RD '%s'", id.ResourceName)
	policy, err := a.getRemoteAccessParameter(ctx, pv)
	if err != nil {
		return nil, fmt.Errorf("failed to determine access policy: %w", err)
	}

	klog.V(2).Infof("Determine expected accessible topologies for policy '%v'", policy)
	topos, err := a.LinstorClient.GenericAccessibleTopologies(ctx, id.ResourceName, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to determine accessible topologies: %w", err)
	}

	if !AffinityMatchesTopology(pv.Spec.NodeAffinity, topos) {
		klog.V(2).Infof("Observed topology doesn't match affinity, updating PV")
		needsApply = true
	}

	if needsApply {
		klog.V(1).Infof("Need to replace PV '%s' for volume %s", pv.Name, id)
		return operations.ReplacePV(a.KubernetesClient, a.LinstorClient, a.EventRecorder, id, pv, topos), nil
	}

	klog.V(2).Infof("PV '%s' affinity is up-to-date with volume '%s'", pv.Name, id)
	return nil, nil
}

func (a *AffinityReconciler) getRemoteAccessParameter(ctx context.Context, pv *corev1.PersistentVolume) (volume.RemoteAccessPolicy, error) {
	klog.V(3).Infof("Search for access policy in PV annotations")
	for k, val := range pv.Annotations {
		if strings.HasPrefix(k, version.OverrideAnnotationPrefix) {
			var policy volume.RemoteAccessPolicy
			err := (&policy).UnmarshalText([]byte(val))
			if err != nil {
				return nil, fmt.Errorf("failed to parse override access context annotation value '%s': %w", val, err)
			}
			klog.V(3).Infof("Found access policy '%v' in PV annotation '%s'", policy, k)
			return policy, nil
		}
	}

	klog.V(3).Infof("Search for access policy in volume context '%s'", pv.Spec.CSI.VolumeAttributes)
	vc, err := driver.VolumeContextFromMap(pv.Spec.CSI.VolumeAttributes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse volume context: %w", err)
	}

	if vc != nil && vc.RemoteAccessPolicy != nil {
		klog.V(3).Infof("Found for access policy '%v' in volume context", vc.RemoteAccessPolicy)
		return vc.RemoteAccessPolicy, nil
	}

	klog.V(3).Infof("Search for storage class '%s'", pv.Spec.StorageClassName)
	sc, err := a.KubernetesClient.StorageV1().StorageClasses().Get(ctx, pv.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get storage class: %w", err)
	}

	klog.V(3).Infof("Parsing storage class parameters '%s'", sc.Name)
	params, err := volume.NewParameters(sc.Parameters, a.LinstorClient.PropertyNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage class parameters: %w", err)
	}

	return params.AllowRemoteVolumeAccess, nil
}
