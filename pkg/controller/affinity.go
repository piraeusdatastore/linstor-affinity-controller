package controller

import (
	"context"
	"encoding/json"
	"fmt"
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
	resources, err := a.LinstorClient.ResourceDefinitions.GetAll(ctx, client.RDGetAllRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list resource definitions: %w", err)
	}

	var ops []operations.Operation
	for i := range resources {
		resource := &resources[i]

		pvs, _ := a.PVIndexer.GetIndexer().ByIndex("rd", resource.Name)

		// There should be one PV in the normal case, but there may be zero in these cases:
		// * The resource is not actually related to Kubernetes and managed externally
		// * The resource was updated by our reconciler, but the reconciler did not complete for one reason or
		//   another, so we need to continue to restore the PV.
		// Our Operation can deal with a "nil" PV.
		var pv *corev1.PersistentVolume
		if len(pvs) == 1 {
			pv = pvs[0].(*corev1.PersistentVolume).DeepCopy()
		}

		o, err := a.generateOperations(ctx, &resource.ResourceDefinition, pv, now)
		if err != nil {
			klog.V(1).ErrorS(err, "Skipping reconciliation because of error", "RD", resource.Name)
		}

		if o != nil {
			ops = append(ops, *o)
		}
	}

	return ops, nil
}

func (a *AffinityReconciler) generateOperations(ctx context.Context, rd *client.ResourceDefinition, pv *corev1.PersistentVolume, now time.Time) (*operations.Operation, error) {
	needsApply := false
	rawSavedProp, hasSavedProp := rd.Props[version.SavedPVPropKey]

	klog.V(2).Infof("Ensure that we have a matching PV to update for RD '%s'", rd.Name)
	if pv == nil {
		if !hasSavedProp {
			klog.V(1).Infof("Resource '%s' has no matching PV and no saved PV configuration, nothing to do", rd.Name)
			return nil, nil
		}
		klog.V(2).Infof("Decoding saved PV from property")

		// NB: we deserialize a PersistentVolumeApplyConfiguration here as PersistentVolume. This is fine, because
		// the ApplyConfiguration is just missing some optional fields, otherwise they are the same.
		pv = &corev1.PersistentVolume{}
		err := json.Unmarshal([]byte(rawSavedProp), pv)
		if err != nil {
			return nil, fmt.Errorf("failed to parse saved PV resource: %w", err)
		}

		needsApply = true
	} else if hasSavedProp {
		// NB: we deserialize a PersistentVolumeApplyConfiguration here as PersistentVolume. This is fine, because
		// the ApplyConfiguration is just missing some optional fields, otherwise they are the same.
		savedPV := &corev1.PersistentVolume{}
		err := json.Unmarshal([]byte(rawSavedProp), savedPV)
		if err != nil {
			return nil, fmt.Errorf("failed to parse saved PV resource: %w", err)
		}

		if pv.DeletionTimestamp != nil || pv.Spec.PersistentVolumeReclaimPolicy != savedPV.Spec.PersistentVolumeReclaimPolicy {
			klog.V(2).Infof("decoded PV does not match current PV or current PV is being deleted, need to re-apply")
			pv.Spec.PersistentVolumeReclaimPolicy = savedPV.Spec.PersistentVolumeReclaimPolicy
			needsApply = true
		} else {
			klog.V(2).Infof("Need to remove stale PV property from RD")
			return operations.RemovePVProperty(a.LinstorClient, rd.Name), nil
		}
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

	klog.V(2).Infof("Check if LINSTOR API Cache for Resource '%s' has resource available", rd.Name)
	ress, err := a.LinstorClient.Resources.GetAll(ctx, rd.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get resources for resource definition '%s': %w", rd.Name, err)
	}

	if len(ress) == 0 {
		klog.V(2).Infof("RD '%s' not (yet) present in cache, skipping", pv.Name)
		return nil, nil
	}

	klog.V(2).Infof("Determine access policy to apply to RD '%s'", rd.Name)
	policy, err := a.getRemoteAccessParameter(ctx, pv)
	if err != nil {
		return nil, fmt.Errorf("failed to determine access policy: %w", err)
	}

	klog.V(2).Infof("Determine expected accessible topologies for policy '%v'", policy)
	topos, err := a.LinstorClient.GenericAccessibleTopologies(ctx, pv.Spec.CSI.VolumeHandle, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to determine accessible topologies: %w", err)
	}

	if !AffinityMatchesTopology(pv.Spec.NodeAffinity, topos) {
		klog.V(2).Infof("Observed topology doesn't match affinity, updating PV")
		needsApply = true
	}

	if needsApply {
		klog.V(1).Infof("Need to replace PV '%s' for resource '%s'", pv.Name, rd.Name)
		return operations.ReplacePV(a.KubernetesClient, a.LinstorClient, a.EventRecorder, rd.Name, pv, topos), nil
	} else {
		klog.V(2).Infof("PV '%s' affinity is up-to-date with resource '%s'", pv.Name, rd.Name)
		return nil, nil
	}
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
