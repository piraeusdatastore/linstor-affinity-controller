package controller

import (
	"context"
	"net/url"
	"testing"
	"time"

	linstor "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/fakelinstor"
	csilinstor "github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	"github.com/piraeusdatastore/linstor-csi/pkg/topology"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"

	"github.com/piraeusdatastore/linstor-affinity-controller/pkg/version"
)

// newLinstorClient starts an in-memory fake LINSTOR controller and returns a
// HighLevelClient pointed at it. The fake server is shut down when the test
// finishes.
func newLinstorClient(t *testing.T) *hlclient.HighLevelClient {
	t.Helper()

	f := fakelinstor.New()
	t.Cleanup(func() { f.Server.Close() })

	u, err := url.Parse(f.Server.URL)
	if err != nil {
		t.Fatalf("failed to parse fake linstor URL: %v", err)
	}

	c, err := hlclient.NewHighLevelClient(lapi.BaseURL(u), lapi.HTTPClient(f.Server.Client()))
	if err != nil {
		t.Fatalf("failed to build high level client: %v", err)
	}

	return c
}

// satelliteNode returns a satellite node with a network interface, which the
// fake reports as ONLINE.
func satelliteNode(name string) lapi.Node {
	return lapi.Node{
		Name:          name,
		Type:          linstor.ValNodeTypeStlt,
		NetInterfaces: []lapi.NetInterface{{Name: "default"}},
	}
}

func retainedPV(name, handle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: csilinstor.DriverName, VolumeHandle: handle},
			},
		},
	}
}

// newPVIndexer builds a PersistentVolume informer indexed by resource
// definition and seeded with the given PVs.
func newPVIndexer(t *testing.T, pvs ...*corev1.PersistentVolume) cache.SharedIndexInformer {
	t.Helper()

	factory := informers.NewSharedInformerFactory(k8sfake.NewSimpleClientset(), 0)
	inf := factory.Core().V1().PersistentVolumes().Informer()
	if err := inf.AddIndexers(cache.Indexers{"rd": IndexByResourceDefinition}); err != nil {
		t.Fatalf("failed to add indexer: %v", err)
	}
	for _, pv := range pvs {
		if err := inf.GetIndexer().Add(pv); err != nil {
			t.Fatalf("failed to add PV %q: %v", pv.Name, err)
		}
	}

	return inf
}

// TestReconcileCleansStaleStatePerVolume covers a single resource definition
// backing two PVs (two volume numbers): each volume definition carries stale
// saved state whose reclaim policy already matches the live PV, so the
// reconciler should emit a cleanup operation for each and leave both volume
// definitions clean.
func TestReconcileCleansStaleStatePerVolume(t *testing.T) {
	saved := `{"spec":{"persistentVolumeReclaimPolicy":"Retain","csi":{"volumeHandle":"res-1"}}}`

	c := newLinstorClient(t)
	ctx := context.Background()

	if err := c.ResourceDefinitions.Create(ctx, lapi.ResourceDefinitionCreate{
		ResourceDefinition: lapi.ResourceDefinition{Name: "res-1"},
	}); err != nil {
		t.Fatalf("failed to seed resource definition: %v", err)
	}
	for _, volNr := range []int32{0, 1} {
		n := volNr
		if err := c.ResourceDefinitions.CreateVolumeDefinition(ctx, "res-1", lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{VolumeNumber: &n},
		}); err != nil {
			t.Fatalf("failed to seed volume definition %d: %v", volNr, err)
		}
		if err := c.ResourceDefinitions.ModifyVolumeDefinition(ctx, "res-1", int(volNr), lapi.VolumeDefinitionModify{
			GenericPropsModify: lapi.GenericPropsModify{OverrideProps: map[string]string{version.SavedPVPropKey: saved}},
		}); err != nil {
			t.Fatalf("failed to seed saved state on volume definition %d: %v", volNr, err)
		}
	}

	inf := newPVIndexer(t, retainedPV("pv-0", "res-1"), retainedPV("pv-1", "res-1/1"))

	r := &AffinityReconciler{
		PVIndexer:        inf,
		LinstorClient:    c,
		KubernetesClient: k8sfake.NewSimpleClientset(),
		EventRecorder:    events.NewFakeRecorder(10),
	}

	ops, err := r.GenerateOperations(ctx, time.Now())
	if err != nil {
		t.Fatalf("GenerateOperations() error: %v", err)
	}

	if len(ops) != 2 {
		t.Fatalf("got %d operations, want 2", len(ops))
	}
	for _, op := range ops {
		if op.Name != "ResetOperation" {
			t.Errorf("operation name = %q, want ResetOperation", op.Name)
		}
		if err := op.Execute(ctx); err != nil {
			t.Fatalf("operation %q failed: %v", op.Name, err)
		}
	}

	vds, err := c.ResourceDefinitions.GetVolumeDefinitions(ctx, "res-1")
	if err != nil {
		t.Fatalf("failed to get volume definitions: %v", err)
	}
	if len(vds) != 2 {
		t.Fatalf("got %d volume definitions, want 2", len(vds))
	}
	for _, vd := range vds {
		if _, ok := vd.Props[version.SavedPVPropKey]; ok {
			t.Errorf("volume definition %v still has saved PV state, want it cleared", vd.VolumeNumber)
		}
	}
}

// TestReconcileReplacesStaleAffinity covers the end-to-end replace path: a live
// PV whose node affinity no longer matches the topology LINSTOR reports for the
// resource. The reconciler should emit a ReplacePV operation that re-creates the
// PV with affinity pointing at the node where the resource is actually deployed.
func TestReconcileReplacesStaleAffinity(t *testing.T) {
	c := newLinstorClient(t)
	ctx := context.Background()

	if err := c.Nodes.Create(ctx, satelliteNode("node-a")); err != nil {
		t.Fatalf("failed to seed node: %v", err)
	}
	if err := c.ResourceDefinitions.Create(ctx, lapi.ResourceDefinitionCreate{
		ResourceDefinition: lapi.ResourceDefinition{Name: "pvc-1"},
	}); err != nil {
		t.Fatalf("failed to seed resource definition: %v", err)
	}
	volNr := int32(0)
	if err := c.ResourceDefinitions.CreateVolumeDefinition(ctx, "pvc-1", lapi.VolumeDefinitionCreate{
		VolumeDefinition: lapi.VolumeDefinition{VolumeNumber: &volNr},
	}); err != nil {
		t.Fatalf("failed to seed volume definition: %v", err)
	}
	if err := c.Resources.Create(ctx, lapi.ResourceCreate{
		Resource: lapi.Resource{Name: "pvc-1", NodeName: "node-a"},
	}); err != nil {
		t.Fatalf("failed to seed resource: %v", err)
	}

	// The resource is deployed diskfully on node-a, so with a local-only access
	// policy the only accessible topology is node-a. The PV still points at
	// node-b, so it is stale.
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pvc-1",
			UID:             "uid-1",
			ResourceVersion: "1",
			Annotations:     map[string]string{version.OverrideAnnotationPrefix + "/allow-remote-access": "false"},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: csilinstor.DriverName, VolumeHandle: "pvc-1"},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{
					{Key: topology.LinstorNodeKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"node-b"}},
				}},
			}}},
		},
	}

	kc := k8sfake.NewClientset(pv)
	inf := newPVIndexer(t, pv)

	r := &AffinityReconciler{
		PVIndexer:        inf,
		LinstorClient:    c,
		KubernetesClient: kc,
		EventRecorder:    events.NewFakeRecorder(10),
	}

	ops, err := r.GenerateOperations(ctx, time.Now())
	if err != nil {
		t.Fatalf("GenerateOperations() error: %v", err)
	}

	if len(ops) != 1 {
		t.Fatalf("got %d operations, want 1", len(ops))
	}
	if ops[0].Name != "ReplacePV" {
		t.Fatalf("operation name = %q, want ReplacePV", ops[0].Name)
	}
	if err := ops[0].Execute(ctx); err != nil {
		t.Fatalf("ReplacePV failed: %v", err)
	}

	got, err := kc.CoreV1().PersistentVolumes().Get(ctx, "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get replaced PV: %v", err)
	}

	if got.Spec.NodeAffinity == nil || got.Spec.NodeAffinity.Required == nil {
		t.Fatalf("replaced PV has no node affinity: %+v", got.Spec.NodeAffinity)
	}
	terms := got.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("unexpected node selector terms: %+v", terms)
	}
	if expr := terms[0].MatchExpressions[0]; expr.Key != topology.LinstorNodeKey || len(expr.Values) != 1 || expr.Values[0] != "node-a" {
		t.Errorf("node affinity = %+v, want %s In [node-a]", expr, topology.LinstorNodeKey)
	}

	// The original reclaim policy is restored after the swap.
	if got.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("reclaim policy = %q, want %q", got.Spec.PersistentVolumeReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
	}
}
