package main

import (
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	k8scli "k8s.io/component-base/cli"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/controller"
	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

func NewControllerCommand() *cobra.Command {
	cfgflags := genericclioptions.NewConfigFlags(false)
	var reconcileRate, resyncRate, timeout time.Duration

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

			ctrl, err := controller.NewReconciler(&controller.Config{
				RestCfg:       cfg,
				ResyncRate:    resyncRate,
				ReconcileRate: reconcileRate,
				Timeout:       timeout,
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
	return cmd
}

func main() {
	exitcode := k8scli.Run(NewControllerCommand())
	os.Exit(exitcode)
}
