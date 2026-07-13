package controller

import (
	"reflect"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csilinstor "github.com/piraeusdatastore/linstor-csi/pkg/linstor"
	corev1 "k8s.io/api/core/v1"
)

func linstorPV(handle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: csilinstor.DriverName, VolumeHandle: handle},
			},
		},
	}
}

func TestIndexByResourceDefinition(t *testing.T) {
	for _, tc := range []struct {
		name    string
		obj     interface{}
		want    []string
		wantErr bool
	}{
		{
			name: "not a persistent volume",
			obj:  &corev1.Pod{},
			want: nil,
		},
		{
			name: "persistent volume without CSI source",
			obj:  &corev1.PersistentVolume{},
			want: nil,
		},
		{
			name: "persistent volume from another driver",
			obj: &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: "ebs.csi.aws.com", VolumeHandle: "vol-123"},
				},
			}},
			want: nil,
		},
		{
			name: "handle without volume suffix",
			obj:  linstorPV("pvc-abc"),
			want: []string{"pvc-abc"},
		},
		{
			name: "handle with volume suffix indexes by resource name",
			obj:  linstorPV("pvc-abc/3"),
			want: []string{"pvc-abc"},
		},
		{
			name:    "unparseable handle",
			obj:     linstorPV(""),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := IndexByResourceDefinition(tc.obj)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("IndexByResourceDefinition() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("IndexByResourceDefinition() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("IndexByResourceDefinition() = %v, want %v", got, tc.want)
			}
		})
	}
}

func affinityFor(segs ...map[string]string) *corev1.VolumeNodeAffinity {
	terms := make([]corev1.NodeSelectorTerm, 0, len(segs))
	for _, seg := range segs {
		exprs := make([]corev1.NodeSelectorRequirement, 0, len(seg))
		for k, v := range seg {
			exprs = append(exprs, corev1.NodeSelectorRequirement{
				Key:      k,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{v},
			})
		}
		terms = append(terms, corev1.NodeSelectorTerm{MatchExpressions: exprs})
	}

	return &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: terms}}
}

func topos(segs ...map[string]string) []*csi.Topology {
	out := make([]*csi.Topology, 0, len(segs))
	for _, seg := range segs {
		out = append(out, &csi.Topology{Segments: seg})
	}

	return out
}

func TestAffinityMatchesTopology(t *testing.T) {
	hostA := map[string]string{"kubernetes.io/hostname": "a"}
	hostB := map[string]string{"kubernetes.io/hostname": "b"}

	for _, tc := range []struct {
		name     string
		affinity *corev1.VolumeNodeAffinity
		topos    []*csi.Topology
		want     bool
	}{
		{name: "nil affinity and no topologies", affinity: nil, topos: nil, want: true},
		{name: "nil affinity with topologies", affinity: nil, topos: topos(hostA), want: false},
		{name: "empty required with topologies", affinity: &corev1.VolumeNodeAffinity{}, topos: topos(hostA), want: false},
		{name: "single match", affinity: affinityFor(hostA), topos: topos(hostA), want: true},
		{name: "value mismatch", affinity: affinityFor(hostA), topos: topos(hostB), want: false},
		{name: "affinity term missing from topologies", affinity: affinityFor(hostA, hostB), topos: topos(hostA), want: false},
		{name: "topology missing from affinity", affinity: affinityFor(hostA), topos: topos(hostA, hostB), want: false},
		{name: "two matching terms", affinity: affinityFor(hostA, hostB), topos: topos(hostA, hostB), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := AffinityMatchesTopology(tc.affinity, tc.topos); got != tc.want {
				t.Errorf("AffinityMatchesTopology() = %v, want %v", got, tc.want)
			}
		})
	}
}
