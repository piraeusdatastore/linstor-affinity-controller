package leaderelection

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

type Config struct {
	leaderElection                   bool
	leaderElectionLeaseLockName      string
	leaderElectionLeaseLockNamespace string
	leaderElectionId                 string
	leaderElectionLeaseDuration      time.Duration
	leaderElectionRenewDeadline      time.Duration
	leaderElectionRetryPeriod        time.Duration
}

func (c *Config) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&c.leaderElection, "leader-election", false, "use leader election to safely coordinate multiple instances")
	fs.StringVar(&c.leaderElectionId, "leader-election-id", "", "the leader election identity")
	fs.StringVar(&c.leaderElectionLeaseLockName, "lease-lock-name", "", "the leader election lease lock resource name")
	fs.StringVar(&c.leaderElectionLeaseLockNamespace, "lease-lock-namespace", "", "the leader election lease lock resource namespace")
	fs.DurationVar(&c.leaderElectionLeaseDuration, "leader-election-lease-duration", 30*time.Second, "Duration, in seconds, that non-leader candidates will wait to force acquire leadership.")
	fs.DurationVar(&c.leaderElectionRenewDeadline, "leader-election-renew-deadline", 25*time.Second, "Duration, in seconds, that the acting leader will retry refreshing leadership before giving up.")
	fs.DurationVar(&c.leaderElectionRetryPeriod, "leader-election-retry-period", 10*time.Second, "Duration, in seconds, the LeaderElector clients should wait between tries of actions.")
}

func (c *Config) MakeElector(cancelFunc func(), cfg *rest.Config) (*leaderelection.LeaderElector, error) {
	kclient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	if !c.leaderElection {
		klog.V(2).Infof("leader election disabled")
		return nil, nil
	}

	if c.leaderElectionLeaseLockName == "" {
		c.leaderElectionLeaseLockName = os.Getenv("LEASE_LOCK_NAME")
	}

	if c.leaderElectionId == "" {
		c.leaderElectionId = os.Getenv("LEASE_HOLDER_IDENTITY")
	}

	if c.leaderElectionLeaseLockNamespace == "" {
		c.leaderElectionLeaseLockNamespace = inClusterNamespace()
	}

	rlConfig := resourcelock.ResourceLockConfig{
		Identity: c.leaderElectionId,
	}

	lock, err := resourcelock.New(resourcelock.LeasesResourceLock, c.leaderElectionLeaseLockNamespace, c.leaderElectionLeaseLockName, kclient.CoreV1(), kclient.CoordinationV1(), rlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create lease lock: %w", err)
	}

	leaderConfig := leaderelection.LeaderElectionConfig{
		Name:          c.leaderElectionLeaseLockName,
		Lock:          lock,
		LeaseDuration: c.leaderElectionLeaseDuration,
		RenewDeadline: c.leaderElectionRenewDeadline,
		RetryPeriod:   c.leaderElectionRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.V(2).Info("became leader, starting")
			},
			OnStoppedLeading: func() {
				cancelFunc()
				klog.V(1).Infof("stopped leading")
			},
			OnNewLeader: func(identity string) {
				klog.V(3).Infof("new leader detected, current leader: %s", identity)
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create leader elector: %w", err)
	}

	return elector, nil
}

func inClusterNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns
		}
	}

	return corev1.NamespaceDefault
}
