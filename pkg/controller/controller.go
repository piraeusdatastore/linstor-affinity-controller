package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	linstor "github.com/LINBIT/golinstor"
	clientcache "github.com/LINBIT/golinstor/cache"
	"github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	csilinstor "github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog/v2"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/operations"
	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

type Config struct {
	RestCfg           *rest.Config
	ReconcileRate     time.Duration
	ResyncRate        time.Duration
	Timeout           time.Duration
	LeaderElector     *leaderelection.LeaderElector
	BindAddress       string
	MetricsAddress    string
	PropertyNamespace string
	Workers           int
}

type AffinityController struct {
	config          *Config
	reconcilers     []Reconciler
	informerFactory informers.SharedInformerFactory
	broadcaster     events.EventBroadcaster
}

type Reconciler interface {
	Name() string
	GenerateOperations(ctx context.Context, now time.Time) ([]operations.Operation, error)
}

func New(cfg *Config) (*AffinityController, error) {
	lclient, err := hlclient.NewHighLevelClient(
		client.Log(KLogV(3)),
		client.UserAgent("linstor-affinity-controller/"+version.Version),
		clientcache.WithCaches(&clientcache.NodeCache{Timeout: cfg.Timeout}),
		clientcache.WithCaches(&clientcache.ResourceCache{Timeout: cfg.Timeout}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize linstor client: %w", err)
	}

	if cfg.PropertyNamespace == "" {
		prop, err := guessPropertyNamespace(context.Background(), lclient)
		if err != nil {
			return nil, fmt.Errorf("failed to guess property namespace: %w", err)
		}

		klog.V(1).Infof("Determined property namespace: '%s'", prop)
		cfg.PropertyNamespace = prop
	}

	lclient.PropertyNamespace = cfg.PropertyNamespace

	kclient, err := kubernetes.NewForConfig(cfg.RestCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kubernetes client: %w", err)
	}

	factory := informers.NewSharedInformerFactory(kclient, cfg.ResyncRate)
	pvIndexer := factory.Core().V1().PersistentVolumes().Informer()

	err = pvIndexer.AddIndexers(map[string]cache.IndexFunc{
		"rd": IndexByResourceDefinition,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initiliaze pv indexer: %w", err)
	}

	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: kclient.EventsV1()})
	recorder := broadcaster.NewRecorder(scheme.Scheme, version.EventsReporter)

	reconcilers := []Reconciler{
		&AffinityReconciler{
			PVIndexer:        pvIndexer,
			LinstorClient:    lclient,
			KubernetesClient: kclient,
			EventRecorder:    recorder,
			NewPVTimeout:     cfg.Timeout,
		},
	}

	a := &AffinityController{
		config:          cfg,
		informerFactory: factory,
		broadcaster:     broadcaster,
		reconcilers:     reconcilers,
	}

	return a, nil
}

func (a *AffinityController) Run(ctx context.Context) error {
	a.broadcaster.StartRecordingToSink(ctx.Done())
	defer a.broadcaster.Shutdown()

	if a.config.BindAddress != "" {
		err := a.runLivenessEndpoints(ctx)
		if err != nil {
			return fmt.Errorf("failed to start liveness server: %w", err)
		}
	}

	var reconcilerCounter, reconcilerFailCounter, reconcilerGeneratedOperationGauge, operationCounter, operationFailCounter *prometheus.CounterVec
	var reconcilerDuration, operationDuration *prometheus.HistogramVec
	if a.config.MetricsAddress != "" {
		reconcilerCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "linstor_affinity_controller_reconciles_total",
			Help: "Total number of reconciles",
		}, []string{"reconciler"})
		reconcilerFailCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "linstor_affinity_controller_reconciles_failures_total",
			Help: "Total number of failed reconciles",
		}, []string{"reconciler"})
		operationCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "linstor_affinity_controller_operations_total",
			Help: "Total number of started operations",
		}, []string{"operation"})
		operationFailCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "linstor_affinity_controller_operations_failures_total",
			Help: "Total number of failed operations",
		}, []string{"operation"})
		reconcilerGeneratedOperationGauge = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "linstor_affinity_controller_operations_per_reconciler_total",
			Help: "Number of operations generated per reconciler",
		}, []string{"reconciler", "operation"})
		reconcilerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "linstor_affinity_controller_reconciles_duration",
			Help: "Duration of the reconciliation",
		}, []string{"reconciler"})
		operationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "linstor_affinity_controller_operations_duration",
			Help: "Duration of the operation",
		}, []string{"operation"})

		promRegistry := prometheus.NewPedanticRegistry()
		promRegistry.MustRegister(collectors.NewGoCollector())
		promRegistry.MustRegister(
			reconcilerCounter,
			reconcilerFailCounter,
			reconcilerGeneratedOperationGauge,
			operationCounter,
			operationFailCounter,
			reconcilerDuration,
			operationDuration,
		)

		err := a.runMetricsEndpoints(ctx, promRegistry)
		if err != nil {
			return fmt.Errorf("failed to start metrics server: %w", err)
		}
	}

	if a.config.LeaderElector != nil {
		klog.V(2).Infof("starting leader election")
		go a.config.LeaderElector.Run(ctx)
	}

	a.informerFactory.Start(ctx.Done())
	a.informerFactory.WaitForCacheSync(ctx.Done())

	ticker := time.NewTicker(a.config.ReconcileRate)

	for {
		now := time.Now()
		runCtx, cancel := context.WithTimeout(ctx, a.config.Timeout)

		var reconcileEG errgroup.Group
		reconcileEG.SetLimit(a.config.Workers)
		ops := make([][]operations.Operation, len(a.reconcilers))
		for i := range a.reconcilers {
			re := a.reconcilers[i]
			reconcileEG.Go(func() error {
				if reconcilerCounter != nil {
					reconcilerCounter.WithLabelValues(re.Name()).Inc()
					timer := prometheus.NewTimer(reconcilerDuration.WithLabelValues(re.Name()))
					defer timer.ObserveDuration()
				}

				o, err := re.GenerateOperations(runCtx, now)
				if err != nil {
					if reconcilerFailCounter != nil {
						reconcilerFailCounter.WithLabelValues(re.Name()).Inc()
					}
					return fmt.Errorf("failed to run reconciler: '%w'", err)
				}

				if reconcilerGeneratedOperationGauge != nil {
					perOps := make(map[string]int)
					for _, op := range o {
						perOps[op.Name]++
					}
					for k, v := range perOps {
						reconcilerGeneratedOperationGauge.WithLabelValues(re.Name(), k).Add(float64(v))
					}
				}
				ops[i] = o
				return nil
			})
		}
		err := reconcileEG.Wait()
		if err != nil {
			klog.V(1).ErrorS(err, "failed to run some reconcilers")
		}

		cancel()
		runCtx, cancel = context.WithTimeout(ctx, a.config.Timeout)
		var operationEG errgroup.Group
		operationEG.SetLimit(a.config.Workers)

		for _, op := range slices.Concat(ops...) {
			operationEG.Go(func() error {
				if a.config.LeaderElector != nil && !a.config.LeaderElector.IsLeader() {
					klog.V(2).Infof("Not leading, not executing operation")
					return nil
				}

				if operationCounter != nil {
					operationCounter.WithLabelValues(op.Name).Inc()
					timer := prometheus.NewTimer(operationDuration.WithLabelValues(op.Name))
					defer timer.ObserveDuration()
				}

				err := op.Execute(runCtx)
				if err != nil {
					operationFailCounter.WithLabelValues(op.Name).Inc()
				}

				return err
			})
		}

		err = operationEG.Wait()
		if err != nil {
			klog.V(1).ErrorS(err, "failed to reconcile some resources")
		}

		cancel()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}

func (a *AffinityController) runLivenessEndpoints(ctx context.Context) error {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", a.config.BindAddress)
	if err != nil {
		return fmt.Errorf("failed to create endpoint: %w", err)
	}

	healthz := leaderelection.NewLeaderHealthzAdaptor(5 * time.Second)
	if a.config.LeaderElector != nil {
		healthz.SetLeaderElection(a.config.LeaderElector)
	}

	mux := &http.ServeMux{}
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		err := healthz.Check(request)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintf(writer, "healthz check failed: %v", err)
			return
		}
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		c := make(chan struct{}, 1)
		close(c)
		caches := a.informerFactory.WaitForCacheSync(c)
		for ty, synced := range caches {
			if !synced {
				writer.WriteHeader(http.StatusInternalServerError)
				_, _ = fmt.Fprintf(writer, "cache for %s not ready", ty)
				return
			}
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	})

	go func() {
		klog.V(1).Infof("Start serving /healthz and /readyz on %s", a.config.BindAddress)
		err := http.Serve(listener, mux)
		if err != nil {
			klog.V(2).ErrorS(err, "liveness server exited with error")
		}
	}()

	return nil
}

func (a *AffinityController) runMetricsEndpoints(ctx context.Context, promRegistry *prometheus.Registry) error {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", a.config.MetricsAddress)
	if err != nil {
		return fmt.Errorf("failed to create endpoint: %w", err)
	}

	mux := &http.ServeMux{}
	mux.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{Registry: promRegistry}))
	go func() {
		klog.V(1).Infof("Start serving /metrics on %s", a.config.MetricsAddress)
		err := http.Serve(listener, mux)
		if err != nil {
			klog.V(2).ErrorS(err, "metrics server exited with error")
		}
	}()

	return nil
}

func IndexByResourceDefinition(obj interface{}) ([]string, error) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok {
		return nil, nil
	}

	if pv.Spec.CSI == nil {
		return nil, nil
	}

	if pv.Spec.CSI.Driver != csilinstor.DriverName {
		return nil, nil
	}

	return []string{pv.Spec.CSI.VolumeHandle}, nil
}

func AffinityMatchesTopology(affinity *corev1.VolumeNodeAffinity, topos []*csi.Topology) bool {
	// NB: guard against these nils
	if affinity == nil || affinity.Required == nil {
		return len(topos) == 0
	}

	for _, seg := range topos {
		hasMatchingTerm := false
		for _, term := range affinity.Required.NodeSelectorTerms {
			if EqualSelection(seg.GetSegments(), term) {
				hasMatchingTerm = true
				break
			}
		}

		if !hasMatchingTerm {
			return false
		}
	}

	for _, term := range affinity.Required.NodeSelectorTerms {
		hasMatchingTopo := false
		for _, seg := range topos {
			if EqualSelection(seg.GetSegments(), term) {
				hasMatchingTopo = true
				break
			}
		}

		if !hasMatchingTopo {
			return false
		}
	}

	return true
}

func EqualSelection(segments map[string]string, term corev1.NodeSelectorTerm) bool {
	if len(term.MatchFields) != 0 {
		return false
	}

	if len(segments) != len(term.MatchExpressions) {
		return false
	}

	for _, clause := range term.MatchExpressions {
		if clause.Operator != corev1.NodeSelectorOpIn {
			return false
		}

		if len(clause.Values) != 1 {
			return false
		}

		val, ok := segments[clause.Key]
		if !ok {
			return false
		}

		if val != clause.Values[0] {
			return false
		}
	}

	return true
}

func guessPropertyNamespace(ctx context.Context, lclient *hlclient.HighLevelClient) (string, error) {
	nodes, err := lclient.Nodes.GetAll(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list nodes for determining property namespace: %w", err)
	}

	for i := range nodes {
		node := &nodes[i]
		if node.Props[linstor.NamespcAuxiliary+"/topology/kubernetes.io/hostname"] != "" {
			return linstor.NamespcAuxiliary + "/topology", nil
		}
	}

	return linstor.NamespcAuxiliary, nil
}
