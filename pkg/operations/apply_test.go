package operations

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
)

func TestApplyAffinity(t *testing.T) {
	t.Run("nil for no topologies", func(t *testing.T) {
		if got := ApplyAffinity(nil); got != nil {
			t.Errorf("ApplyAffinity(nil) = %v, want nil", got)
		}
		if got := ApplyAffinity([]*csi.Topology{}); got != nil {
			t.Errorf("ApplyAffinity([]) = %v, want nil", got)
		}
	})

	t.Run("expressions sorted by key", func(t *testing.T) {
		got := ApplyAffinity([]*csi.Topology{
			{Segments: map[string]string{"zone": "z1", "kubernetes.io/hostname": "node-a"}},
		})

		if got == nil || got.Required == nil {
			t.Fatalf("ApplyAffinity() = %v, want a required node selector", got)
		}

		terms := got.Required.NodeSelectorTerms
		if len(terms) != 1 {
			t.Fatalf("got %d node selector terms, want 1", len(terms))
		}

		exprs := terms[0].MatchExpressions
		if len(exprs) != 2 {
			t.Fatalf("got %d match expressions, want 2", len(exprs))
		}

		if *exprs[0].Key != "kubernetes.io/hostname" || *exprs[1].Key != "zone" {
			t.Errorf("expressions not sorted by key: %q, %q", *exprs[0].Key, *exprs[1].Key)
		}

		for _, expr := range exprs {
			if expr.Operator == nil || *expr.Operator != corev1.NodeSelectorOpIn {
				t.Errorf("expression %q operator = %v, want In", *expr.Key, expr.Operator)
			}
			if len(expr.Values) != 1 {
				t.Errorf("expression %q has %d values, want 1", *expr.Key, len(expr.Values))
			}
		}
	})
}

func TestApplyCSI(t *testing.T) {
	if got := ApplyCSI(nil); got != nil {
		t.Fatalf("ApplyCSI(nil) = %v, want nil", got)
	}

	src := &corev1.CSIPersistentVolumeSource{
		Driver:       "linstor.csi.linbit.com",
		VolumeHandle: "pvc-abc/2",
		FSType:       "ext4",
		ReadOnly:     true,
	}

	got := ApplyCSI(src)
	if got == nil {
		t.Fatal("ApplyCSI() = nil, want non-nil")
	}
	if got.Driver == nil || *got.Driver != src.Driver {
		t.Errorf("Driver = %v, want %q", got.Driver, src.Driver)
	}
	if got.VolumeHandle == nil || *got.VolumeHandle != src.VolumeHandle {
		t.Errorf("VolumeHandle = %v, want %q", got.VolumeHandle, src.VolumeHandle)
	}
	if got.FSType == nil || *got.FSType != src.FSType {
		t.Errorf("FSType = %v, want %q", got.FSType, src.FSType)
	}
	if got.ReadOnly == nil || *got.ReadOnly != src.ReadOnly {
		t.Errorf("ReadOnly = %v, want %v", got.ReadOnly, src.ReadOnly)
	}
}

func TestApplyClaimRef(t *testing.T) {
	if got := ApplyClaimRef(nil); got != nil {
		t.Fatalf("ApplyClaimRef(nil) = %v, want nil", got)
	}

	ref := &corev1.ObjectReference{Name: "my-pvc", Namespace: "team", UID: "uid-1"}

	got := ApplyClaimRef(ref)
	if got == nil {
		t.Fatal("ApplyClaimRef() = nil, want non-nil")
	}
	if got.Name == nil || *got.Name != ref.Name {
		t.Errorf("Name = %v, want %q", got.Name, ref.Name)
	}
	if got.Namespace == nil || *got.Namespace != ref.Namespace {
		t.Errorf("Namespace = %v, want %q", got.Namespace, ref.Namespace)
	}
	if got.UID == nil || *got.UID != ref.UID {
		t.Errorf("UID = %v, want %q", got.UID, ref.UID)
	}
}

func TestApplySecretRef(t *testing.T) {
	if got := ApplySecretRef(nil); got != nil {
		t.Fatalf("ApplySecretRef(nil) = %v, want nil", got)
	}

	ref := &corev1.SecretReference{Name: "creds", Namespace: "team"}

	got := ApplySecretRef(ref)
	if got == nil {
		t.Fatal("ApplySecretRef() = nil, want non-nil")
	}
	if got.Name == nil || *got.Name != ref.Name {
		t.Errorf("Name = %v, want %q", got.Name, ref.Name)
	}
	if got.Namespace == nil || *got.Namespace != ref.Namespace {
		t.Errorf("Namespace = %v, want %q", got.Namespace, ref.Namespace)
	}
}
