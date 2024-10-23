package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"time"

	linstor "github.com/LINBIT/golinstor"
	clientcache "github.com/LINBIT/golinstor/cache"
	"github.com/LINBIT/golinstor/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/piraeusdatastore/linstor-csi/pkg/driver"
	csilinstor "github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	applyv1core "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog/v2"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

const (
	SavedPVPropKey = linstor.NamespcAuxiliary + "/affinity-updater-saved-pv"
)

type Config struct {
	RestCfg           *rest.Config
	ReconcileRate     time.Duration
	ResyncRate        time.Duration
	Timeout           time.Duration
	LeaderElector     *leaderelection.LeaderElector
	BindAddress       string
	PropertyNamespace string
	Workers           int
}

type AffinityReconciler struct {
	config          *Config
	informerFactory informers.SharedInformerFactory
	pvIndexer       cache.SharedIndexInformer
	lclient         *hlclient.HighLevelClient
	kclient         kubernetes.Interface
	broadcaster     events.EventBroadcaster
	recorder        events.EventRecorder
}

func NewReconciler(cfg *Config) (*AffinityReconciler, error) {
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

	a := &AffinityReconciler{
		config:          cfg,
		informerFactory: factory,
		pvIndexer:       pvIndexer,
		lclient:         lclient,
		kclient:         kclient,
		broadcaster:     broadcaster,
		recorder:        recorder,
	}

	if cfg.BindAddress != "" {
		err := a.runHttpServer()
		if err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *AffinityReconciler) Run(ctx context.Context) error {
	a.broadcaster.StartRecordingToSink(ctx.Done())
	defer a.broadcaster.Shutdown()

	if a.config.LeaderElector != nil {
		klog.V(2).Infof("starting leader election")
		go a.config.LeaderElector.Run(ctx)
	}

	a.informerFactory.Start(ctx.Done())
	a.informerFactory.WaitForCacheSync(ctx.Done())

	ticker := time.NewTicker(a.config.ReconcileRate)

	for {
		runCtx, cancel := context.WithTimeout(ctx, a.config.Timeout)

		resources, err := a.lclient.ResourceDefinitions.GetAll(runCtx, client.RDGetAllRequest{})
		if err != nil {
			klog.V(1).ErrorS(err, "failed to list LINSTOR resource definitions")
		}

		eg, egCtx := errgroup.WithContext(runCtx)
		eg.SetLimit(a.config.Workers)

		for i := range resources {
			resource := &resources[i]

			eg.Go(func() error {
				pvs, _ := a.pvIndexer.GetIndexer().ByIndex("rd", resource.Name)

				// There should be one PV in the normal case, but there may be zero in these cases:
				// * The resource is not actually related to Kubernetes and managed externally
				// * The resource was updated by our reconciler, but the reconciler did not complete for one reason or
				//   another, so we need to continue to restore the PV.
				// reconcileOne() can deal with a "nil" PV.
				var pv *corev1.PersistentVolume
				if len(pvs) == 1 {
					pv = pvs[0].(*corev1.PersistentVolume)
				}

				err := a.reconcileOne(egCtx, &resource.ResourceDefinition, pv)
				if err != nil {
					klog.V(1).ErrorS(err, "failed to reconcile resource", "resource", resource.Name)
				}

				return nil
			})
		}

		err = eg.Wait()
		if err != nil {
			klog.V(1).ErrorS(err, "failed to reconcile all resources")
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

func (a *AffinityReconciler) reconcileOne(ctx context.Context, rd *client.ResourceDefinition, pv *corev1.PersistentVolume) error {
	if a.config.LeaderElector != nil && !a.config.LeaderElector.IsLeader() {
		klog.V(2).Infof("Not leading, not reconciling resource '%s'", rd.Name)
		return nil
	}

	needsApply := false

	rawSavedProp, hasSavedProp := rd.Props[SavedPVPropKey]

	klog.V(2).Infof("Ensure that we have a matching PV to update for RD '%s'", rd.Name)
	if pv == nil {
		if !hasSavedProp {
			klog.V(1).Infof("Resource '%s' has no matching PV and no saved PV configuration, nothing to do", rd.Name)
			return nil
		}
		klog.V(2).Infof("Decoding saved PV from property")

		// NB: we deserialize a PersistentVolumeApplyConfiguration here as PersistentVolume. This is fine, because
		// the ApplyConfiguration is just missing some optional fields, otherwise they are the same.
		pv = &corev1.PersistentVolume{}
		err := json.Unmarshal([]byte(rawSavedProp), pv)
		if err != nil {
			return fmt.Errorf("failed to parse saved PV resource: %w", err)
		}

		needsApply = true
	} else if hasSavedProp {
		klog.V(2).Infof("removing stale PV property from RD")
		err := a.lclient.ResourceDefinitions.Modify(ctx, rd.Name, client.GenericPropsModify{DeleteProps: []string{SavedPVPropKey}})
		if err != nil {
			return fmt.Errorf("failed to remove stale property key: %w", err)
		}
	}

	if pv.Spec.CSI == nil {
		klog.V(2).Infof("PV '%s' not provisioned by CSI, skipping", pv.Name)
		return nil
	}

	if pv.Spec.CSI.Driver != csilinstor.DriverName {
		klog.V(2).Infof("PV '%s' not provisioned by LINSTOR CSI, skipping", pv.Name)
		return nil
	}

	klog.V(2).Infof("Check if LINSTOR API Cache for Resource '%s' has resource available", rd.Name)
	ress, err := a.lclient.Resources.GetAll(ctx, rd.Name)
	if err != nil {
		return fmt.Errorf("failed to get resources for resource definition '%s': %w", rd.Name, err)
	}

	if len(ress) == 0 {
		klog.V(2).Infof("RD '%s' not (yet) present in cache, skipping", pv.Name)
		return nil
	}

	klog.V(2).Infof("Determine access policy to apply to RD '%s'", rd.Name)
	policy, err := a.getRemoteAccessParameter(ctx, pv)
	if err != nil {
		return fmt.Errorf("failed to determine access policy: %w", err)
	}

	klog.V(2).Infof("Determine expected accessible topologies for policy '%v'", policy)
	topos, err := a.lclient.GenericAccessibleTopologies(ctx, pv.Spec.CSI.VolumeHandle, policy)
	if err != nil {
		return fmt.Errorf("failed to determine accessible topologies: %w", err)
	}

	if !AffinityMatchesTopology(pv.Spec.NodeAffinity, topos) {
		klog.V(2).Infof("Observed topology doesn't match affinity, updating PV")
		needsApply = true
	}

	if needsApply {
		klog.V(1).Infof("Need to replace PV '%s' for resource '%s'", pv.Name, rd.Name)

		err := a.replacePV(ctx, rd.Name, pv, topos)
		if err != nil {
			return fmt.Errorf("failed to replace PV: %w", err)
		}

		a.recorder.Eventf(pv, nil, corev1.EventTypeNormal, "VolumeAffinityUpdated", "Replaced with updated version", "Affinity was out of sync with LINSTOR resource state")
		if pv.Spec.ClaimRef != nil {
			a.recorder.Eventf(pv.Spec.ClaimRef, nil, corev1.EventTypeNormal, "VolumeAffinityUpdated", "Replaced with updated version", "Affinity was out of sync with LINSTOR resource state")
		}
	} else {
		klog.V(2).Infof("PV '%s' affinity is up-to-date with resource '%s'", pv.Name, rd.Name)
	}

	return nil
}

func (a *AffinityReconciler) getRemoteAccessParameter(ctx context.Context, pv *corev1.PersistentVolume) (volume.RemoteAccessPolicy, error) {
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
	sc, err := a.kclient.StorageV1().StorageClasses().Get(ctx, pv.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get storage class: %w", err)
	}

	klog.V(3).Infof("Parsing storage class parameters '%s'", sc.Name)
	params, err := volume.NewParameters(sc.Parameters, a.lclient.PropertyNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage class parameters: %w", err)
	}

	return params.AllowRemoteVolumeAccess, nil
}

func (a *AffinityReconciler) replacePV(ctx context.Context, rdName string, pv *corev1.PersistentVolume, topologies []*csi.Topology) error {
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

	err = a.lclient.ResourceDefinitions.Modify(ctx, rdName, client.GenericPropsModify{OverrideProps: map[string]string{SavedPVPropKey: string(encoded)}})
	if err != nil {
		return fmt.Errorf("failed to update property value: %w", err)
	}

	// If no resource version is set --> PV already deleted
	if pv.ObjectMeta.ResourceVersion != "" {
		klog.V(3).Infof("Reconfigure PV to not delete backing volume: %v", err)

		patch := []byte(`{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}`)
		pv, err = a.kclient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: version.FieldManager})

		if err != nil {
			return fmt.Errorf("failed to reconfigure PV reclaim mode: %w", err)
		}

		klog.V(3).Infof("Start deletion of PV '%s'", pv.Name)

		err = a.kclient.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID:             &pv.UID,
			ResourceVersion: &pv.ResourceVersion,
		}})

		if err != nil {
			return fmt.Errorf("failed to start deleting PV: %w", err)
		}

		klog.V(2).Infof("Removing finalizers from PV '%s'", pv.Name)

		patch = []byte(`{"metadata":{"finalizers": null}}`)
		_, err = a.kclient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: version.FieldManager})
		if err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to fetch deleting PV: %w", err)
		}
	}

	klog.V(3).Infof("Applying replacement PV")
	_, err = a.kclient.CoreV1().PersistentVolumes().Apply(ctx, newPv, metav1.ApplyOptions{FieldManager: version.FieldManager, Force: true})
	if err != nil {
		return fmt.Errorf("failed to apply replacement PV: %w", err)
	}

	klog.V(3).Infof("removing stale PV property from RD")
	err = a.lclient.ResourceDefinitions.Modify(ctx, rdName, client.GenericPropsModify{DeleteProps: []string{SavedPVPropKey}})
	if err != nil {
		return fmt.Errorf("failed to remove stale property from RD: %w", err)
	}

	return nil
}

func (a *AffinityReconciler) runHttpServer() error {
	listener, err := net.Listen("tcp", a.config.BindAddress)
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
			_, _ = writer.Write([]byte("leader election check failed"))
			return
		}
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if a.pvIndexer.HasSynced() {
			_, _ = writer.Write([]byte("ok"))
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte("not ready"))
		}
	})

	go func() {
		klog.V(1).Infof("Start serving /healthz and /readyz")
		err = http.Serve(listener, mux)
		if err != nil {
			klog.V(2).ErrorS(err, "server exited with error")
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
