package migrate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LINBIT/golinstor/client"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

// SavedPVPropsToVolumeDefinitions moves the saved-PV property that older versions
// stored on the resource definition onto the matching volume definition, where
// current versions expect it.
//
// It is a one-shot upgrade step: a single resource definition can now back
// multiple PVs (one per volume), so the per-PV state can no longer live on the
// shared resource definition. The property is only ever set while a PV is being
// replaced, so on a healthy cluster there is usually nothing to migrate.
//
// The migration is idempotent and crash-safe: the property is written to the
// volume definition before it is removed from the resource definition, so an
// interrupted run leaves the state discoverable and a re-run converges.
func SavedPVPropsToVolumeDefinitions(ctx context.Context, lclient *hlclient.HighLevelClient) error {
	rds, err := lclient.ResourceDefinitions.GetAll(ctx, client.RDGetAllRequest{WithVolumeDefinitions: true})
	if err != nil {
		return fmt.Errorf("failed to list resource definitions: %w", err)
	}

	migrated := 0
	for i := range rds {
		rd := &rds[i]

		raw, ok := rd.Props[version.SavedPVPropKey]
		if !ok {
			continue
		}

		id, err := savedID(raw)
		if err != nil {
			return fmt.Errorf("failed to determine volume number of saved PV on resource definition '%s': %w", rd.Name, err)
		}

		if id.ResourceName != rd.Name {
			return fmt.Errorf("inconsistent saved PV state: volume ID '%s' does not match resource definition '%s'", id, rd.Name)
		}

		if hasVolumeDefinitionProp(rd, id.VolumeNumber) {
			klog.Infof("Volume %s already holds saved PV state, dropping stale copy from resource definition", id)
		} else {
			klog.Infof("Moving saved PV state from resource definition '%s' to volume %s", rd.Name, id)
			err = lclient.ResourceDefinitions.ModifyVolumeDefinition(ctx, id.ResourceName, id.VolumeNumber, client.VolumeDefinitionModify{
				GenericPropsModify: client.GenericPropsModify{OverrideProps: map[string]string{version.SavedPVPropKey: raw}},
			})
			if err != nil {
				return fmt.Errorf("failed to copy saved PV state to volume %s: %w", id, err)
			}
		}

		err = lclient.ResourceDefinitions.Modify(ctx, rd.Name, client.GenericPropsModify{DeleteProps: []string{version.SavedPVPropKey}})
		if err != nil {
			return fmt.Errorf("failed to remove saved PV state from resource definition '%s': %w", rd.Name, err)
		}

		migrated++
	}

	klog.Infof("Migrated saved PV state for %d resource definition(s)", migrated)

	return nil
}

// savedID returns the volume.ID the saved PV configuration refers
// to, read from the CSI volume handle it carries.
func savedID(raw string) (volume.ID, error) {
	pv := &corev1.PersistentVolume{}
	err := json.Unmarshal([]byte(raw), pv)
	if err != nil {
		return volume.ID{}, fmt.Errorf("failed to parse saved PV resource: %w", err)
	}

	if pv.Spec.CSI == nil {
		return volume.ID{}, fmt.Errorf("saved PV has no CSI source")
	}

	id, err := volume.ParseVolumeId(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return volume.ID{}, fmt.Errorf("failed to parse volume handle '%s': %w", pv.Spec.CSI.VolumeHandle, err)
	}

	return id, nil
}

func hasVolumeDefinitionProp(rd *client.ResourceDefinitionWithVolumeDefinition, volNr int) bool {
	for _, vd := range rd.VolumeDefinitions {
		if vd.VolumeNumber == nil || int(*vd.VolumeNumber) != volNr {
			continue
		}

		_, ok := vd.Props[version.SavedPVPropKey]

		return ok
	}

	return false
}
