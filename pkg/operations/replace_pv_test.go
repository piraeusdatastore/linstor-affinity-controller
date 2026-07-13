package operations

import (
	"context"
	"net/url"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/fakelinstor"
	"github.com/container-storage-interface/spec/lib/go/csi"
	hlclient "github.com/piraeusdatastore/linstor-csi/pkg/linstor/highlevelclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"

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

func TestReplacePVExecute(t *testing.T) {
	existing := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1",
			UID:        "uid-1",
			Finalizers: []string{"kubernetes.io/pv-protection"},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "linstor",
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: "linstor.csi.linbit.com", VolumeHandle: "pvc-1"},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{
					{Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}},
				}},
			}}},
		},
	}

	kc := fake.NewClientset(existing)

	c := newLinstorClient(t)
	ctx := context.Background()

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

	topos := []*csi.Topology{{Segments: map[string]string{"kubernetes.io/hostname": "node-b"}}}

	op := ReplacePV(kc, c, events.NewFakeRecorder(10), volume.ID{ResourceName: "pvc-1"}, existing, topos)

	if err := op.Execute(ctx); err != nil {
		t.Fatalf("ReplacePV.Execute() error: %v", err)
	}

	got, err := kc.CoreV1().PersistentVolumes().Get(ctx, "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get replaced PV: %v", err)
	}

	// Affinity is updated to the new topology.
	if got.Spec.NodeAffinity == nil || got.Spec.NodeAffinity.Required == nil {
		t.Fatalf("replaced PV has no node affinity: %+v", got.Spec.NodeAffinity)
	}
	terms := got.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("unexpected node selector terms: %+v", terms)
	}
	if expr := terms[0].MatchExpressions[0]; expr.Key != "kubernetes.io/hostname" || len(expr.Values) != 1 || expr.Values[0] != "node-b" {
		t.Errorf("node affinity = %+v, want hostname In [node-b]", expr)
	}

	// The original reclaim policy is restored after the swap.
	if got.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("reclaim policy = %q, want %q", got.Spec.PersistentVolumeReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
	}

	// The saved state persisted mid-replace is cleared from the volume definition.
	vds, err := c.ResourceDefinitions.GetVolumeDefinitions(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("failed to get volume definitions: %v", err)
	}
	if len(vds) != 1 {
		t.Fatalf("got %d volume definitions, want 1", len(vds))
	}
	if _, ok := vds[0].Props[version.SavedPVPropKey]; ok {
		t.Errorf("saved PV property still present on volume definition, want it cleared")
	}
}
