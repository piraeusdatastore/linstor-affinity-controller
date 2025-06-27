package operations

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	applyv1core "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

// ReplacePV creates an Operation to replace a PV to update affinity.
//
// To replace a PV, the Operation does:
// 1. Compute the PV that should replace it.
// 2. Save the new PV configuration on the LINSTOR RD as a property (version.SavedPVPropKey).
// 3. Patch the existing PV to use "Retain" reclaim policy.
// 4. Delete the existing PV.
// 5. Force-remove all finalizers from the PV, letting deletion complete.
// 6. Re-apply the updated PV
// 7. Remove the property from the LINSTOR RD.
func ReplacePV(kclient kubernetes.Interface, lclient *hlclient.HighLevelClient, eventRecorder events.EventRecorder, rdName string, pv *corev1.PersistentVolume, topologies []*csi.Topology) *Operation {
	return &Operation{
		Name: "ReplacePV",
		Execute: func(ctx context.Context) error {
			mode := corev1.PersistentVolumeFilesystem
			if pv.Spec.VolumeMode != nil {
				mode = *pv.Spec.VolumeMode
			}

			newPv := applyv1core.PersistentVolume(pv.Name).
				WithAnnotations(pv.Annotations).
				WithLabels(pv.Labels).
				WithFinalizers(pv.Finalizers...).
				WithSpec(
					applyv1core.PersistentVolumeSpec().
						WithAccessModes(pv.Spec.AccessModes...).
						WithCapacity(pv.Spec.Capacity).
						WithClaimRef(ApplyClaimRef(pv.Spec.ClaimRef)).
						WithCSI(ApplyCSI(pv.Spec.CSI)).
						WithPersistentVolumeReclaimPolicy(pv.Spec.PersistentVolumeReclaimPolicy).
						WithStorageClassName(pv.Spec.StorageClassName).
						WithVolumeMode(mode).
						WithNodeAffinity(ApplyAffinity(topologies)),
				)

			klog.V(2).Infof("Will replace PV with '%+v'", newPv)

			klog.V(3).Infof("Saving expected PV state to RD property")

			// NB: We encode a PersistentVolumeApplyConfiguration, but decode a PersistentVolume. These are compatible,
			// but PersistentVolumeApplyConfiguration can have more "empty" fields.
			encoded, err := json.Marshal(newPv)
			if err != nil {
				return fmt.Errorf("failed to encode PV: %w", err)
			}

			err = lclient.ResourceDefinitions.Modify(ctx, rdName, client.GenericPropsModify{OverrideProps: map[string]string{version.SavedPVPropKey: string(encoded)}})
			if err != nil {
				return fmt.Errorf("failed to update property value: %w", err)
			}

			// If no resource version is set --> PV already deleted
			if pv.ObjectMeta.ResourceVersion != "" {
				klog.V(3).Infof("Reconfigure PV to not delete backing volume: %v", err)

				patch := []byte(`{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}`)
				pv, err = kclient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: version.FieldManager})
				if err != nil {
					return fmt.Errorf("failed to reconfigure PV reclaim mode: %w", err)
				}

				klog.V(3).Infof("Start deletion of PV '%s'", pv.Name)
				err = kclient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{
					Preconditions: &metav1.Preconditions{UID: &pv.UID},
				})
				if err != nil {
					return fmt.Errorf("failed to start deleting PV: %w", err)
				}

				klog.V(2).Infof("Removing finalizers from PV '%s'", pv.Name)

				patch = []byte(`{"metadata":{"finalizers": null}}`)
				_, err = kclient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: version.FieldManager})
				if err != nil && !errors.IsNotFound(err) {
					return fmt.Errorf("failed to fetch deleting PV: %w", err)
				}
			}

			klog.V(3).Infof("Applying replacement PV")
			pv, err = kclient.CoreV1().PersistentVolumes().Apply(ctx, newPv, metav1.ApplyOptions{FieldManager: version.FieldManager, Force: true})
			if err != nil {
				return fmt.Errorf("failed to apply replacement PV: %w", err)
			}

			klog.V(3).Infof("removing applied PV property from RD")
			err = lclient.ResourceDefinitions.Modify(ctx, rdName, client.GenericPropsModify{DeleteProps: []string{version.SavedPVPropKey}})
			if err != nil {
				return fmt.Errorf("failed to remove stale property from RD: %w", err)
			}

			eventRecorder.Eventf(pv, nil, corev1.EventTypeNormal, "VolumeAffinityUpdated", "Replaced with updated version", "Affinity was out of sync with LINSTOR resource state")
			if pv.Spec.ClaimRef != nil {
				eventRecorder.Eventf(pv.Spec.ClaimRef, nil, corev1.EventTypeNormal, "VolumeAffinityUpdated", "Replaced with updated version", "Affinity was out of sync with LINSTOR resource state")
			}

			return nil
		},
	}
}

func ApplyClaimRef(reference *corev1.ObjectReference) *applyv1core.ObjectReferenceApplyConfiguration {
	if reference == nil {
		return nil
	}

	return &applyv1core.ObjectReferenceApplyConfiguration{
		Name:            &reference.Name,
		Namespace:       &reference.Namespace,
		APIVersion:      &reference.APIVersion,
		Kind:            &reference.Kind,
		ResourceVersion: &reference.ResourceVersion,
		UID:             &reference.UID,
		FieldPath:       &reference.FieldPath,
	}
}

func ApplyCSI(csi *corev1.CSIPersistentVolumeSource) *applyv1core.CSIPersistentVolumeSourceApplyConfiguration {
	if csi == nil {
		return nil
	}

	return &applyv1core.CSIPersistentVolumeSourceApplyConfiguration{
		VolumeHandle:               &csi.VolumeHandle,
		Driver:                     &csi.Driver,
		ReadOnly:                   &csi.ReadOnly,
		FSType:                     &csi.FSType,
		VolumeAttributes:           csi.VolumeAttributes,
		ControllerPublishSecretRef: ApplySecretRef(csi.ControllerPublishSecretRef),
		ControllerExpandSecretRef:  ApplySecretRef(csi.ControllerExpandSecretRef),
		NodePublishSecretRef:       ApplySecretRef(csi.NodePublishSecretRef),
		NodeStageSecretRef:         ApplySecretRef(csi.NodeStageSecretRef),
	}
}

func ApplySecretRef(ref *corev1.SecretReference) *applyv1core.SecretReferenceApplyConfiguration {
	if ref == nil {
		return nil
	}

	return &applyv1core.SecretReferenceApplyConfiguration{
		Name:      &ref.Name,
		Namespace: &ref.Namespace,
	}
}

func ApplyAffinity(topos []*csi.Topology) *applyv1core.VolumeNodeAffinityApplyConfiguration {
	if len(topos) == 0 {
		return nil
	}

	selector := applyv1core.NodeSelector()

	for _, topo := range topos {
		expressions := make([]*applyv1core.NodeSelectorRequirementApplyConfiguration, 0, len(topo.Segments))
		for k, v := range topo.Segments {
			expressions = append(expressions, applyv1core.NodeSelectorRequirement().
				WithKey(k).
				WithOperator(corev1.NodeSelectorOpIn).
				WithValues(v),
			)
		}

		sort.Slice(expressions, func(i, j int) bool {
			return *expressions[i].Key < *expressions[j].Key
		})

		selector.WithNodeSelectorTerms(applyv1core.NodeSelectorTerm().WithMatchExpressions(expressions...))
	}

	return applyv1core.VolumeNodeAffinity().WithRequired(selector)
}
