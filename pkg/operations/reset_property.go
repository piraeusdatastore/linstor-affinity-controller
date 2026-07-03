package operations

import (
	"context"
	"fmt"

	"github.com/LINBIT/golinstor/client"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

// RemovePVProperty returns an Operation to remove the version.SavedPVPropKey property from a LINSTOR volume definition.
//
// This Operation is a shortcut should the Operation returned by ReplacePV need to be rerun after step 6.
func RemovePVProperty(lclient *hlclient.HighLevelClient, id volume.ID) *Operation {
	return &Operation{
		Name: "ResetOperation",
		Execute: func(ctx context.Context) error {
			err := lclient.ResourceDefinitions.ModifyVolumeDefinition(ctx, id.ResourceName, id.VolumeNumber, client.VolumeDefinitionModify{
				GenericPropsModify: client.GenericPropsModify{DeleteProps: []string{version.SavedPVPropKey}},
			})
			if err != nil {
				return fmt.Errorf("failed to remove stale property key from volume '%s': %w", id, err)
			}

			return nil
		},
	}
}
