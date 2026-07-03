package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/LINBIT/golinstor/client"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	k8scli "k8s.io/component-base/cli"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/controller"
	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/leaderelection"
	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/migrate"
	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

func NewControllerCommand() *cobra.Command {
	cfgflags := genericclioptions.NewConfigFlags(false)
	var reconcileRate, resyncRate, timeout time.Duration
	var electorCfg leaderelection.Config
	var bindAddress, metricsAddress, propertyNamespace string
	var workers int

	cmd := &cobra.Command{
		Use:     "linstor-volume-controller",
		Version: version.Version,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer cancel()

			cfg, err := cfgflags.ToRESTConfig()
			if err != nil {
				return err
			}

			elector, err := electorCfg.MakeElector(cancel, cfg)
			if err != nil {
				return err
			}

			ctrl, err := controller.New(&controller.Config{
				RestCfg:           cfg,
				ResyncRate:        resyncRate,
				ReconcileRate:     reconcileRate,
				Timeout:           timeout,
				LeaderElector:     elector,
				BindAddress:       bindAddress,
				MetricsAddress:    metricsAddress,
				PropertyNamespace: propertyNamespace,
				Workers:           workers,
			})
			if err != nil {
				return err
			}

			return ctrl.Run(ctx)
		},
	}

	cfgflags.AddFlags(cmd.Flags())
	cmd.Flags().DurationVar(&reconcileRate, "reconcile-rate", 15*time.Second, "how often the cluster state should be reconciled")
	cmd.Flags().DurationVar(&resyncRate, "resync-rate", 5*time.Minute, "how often the internal object cache should be resynchronized")
	cmd.Flags().DurationVar(&timeout, "timeout", 1*time.Minute, "how long a single reconcile attempt can take")
	cmd.Flags().StringVar(&bindAddress, "bind-address", "[::]:8000", "the address to use for /healthz and /readyz probes")
	cmd.Flags().StringVar(&metricsAddress, "metrics-address", "", "the address to use for serving /metrics")
	cmd.Flags().StringVar(&propertyNamespace, "property-namespace", "", "The property namespace used by LINSTOR CSI")
	cmd.Flags().IntVar(&workers, "workers", 10, "Number of reconciliations to run in parallel")
	electorCfg.AddFlags(cmd.Flags())

	cmd.AddCommand(NewMigratePropertiesCommand())

	return cmd
}

// NewMigratePropertiesCommand returns a one-shot command that moves the saved-PV
// property from LINSTOR resource definitions to the matching volume definitions.
// It is meant to be run once when upgrading from a version that stored the state
// on the resource definition.
func NewMigratePropertiesCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate-properties",
		Short: "Move saved-PV state from resource definitions to volume definitions",
		Long: "Move the saved-PV state written by older versions from LINSTOR resource definitions to " +
			"the matching volume definitions. Run this once when upgrading. The command is idempotent.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer cancel()

			lclient, err := hlclient.NewHighLevelClient(
				client.Log(controller.KLogV(3)),
				client.UserAgent("linstor-affinity-controller/"+version.Version),
			)
			if err != nil {
				return fmt.Errorf("failed to initialize linstor client: %w", err)
			}

			return migrate.SavedPVPropsToVolumeDefinitions(ctx, lclient)
		},
	}
}

func main() {
	exitcode := k8scli.Run(NewControllerCommand())
	os.Exit(exitcode)
}
