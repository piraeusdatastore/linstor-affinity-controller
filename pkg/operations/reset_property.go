package operations

import (
	"context"
	"fmt"

	"github.com/LINBIT/golinstor/client"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

// RemovePVProperty returns an Operation to remove the version.SavedPVPropKey property from a LINSTOR RD.
//
// This Operation is a shortcut should the Operation returned by ReplacePV need to be rerun after step 6.
func RemovePVProperty(lclient *hlclient.HighLevelClient, rdName string) *Operation {
	return &Operation{
		Name: "ResetOperation",
		Execute: func(ctx context.Context) error {
			err := lclient.ResourceDefinitions.Modify(ctx, rdName, client.GenericPropsModify{DeleteProps: []string{version.SavedPVPropKey}})
			if err != nil {
				return fmt.Errorf("failed to remove stale property key from '%s': %w", rdName, err)
			}

			return nil
		},
	}
}
