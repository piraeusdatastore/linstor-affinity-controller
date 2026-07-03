package migrate

import (
	"testing"

	"github.com/piraeusdatastore/linstor-csi/pkg/volume"
)

func TestSavedID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		want    volume.ID
		wantErr bool
	}{
		{
			name: "handle without volume suffix is volume 0",
			raw:  `{"spec":{"csi":{"volumeHandle":"pvc-11111111-1111-1111-1111-111111111111"}}}`,
			want: volume.ID{ResourceName: "pvc-11111111-1111-1111-1111-111111111111", VolumeNumber: 0},
		},
		{
			name: "handle with volume suffix",
			raw:  `{"spec":{"csi":{"volumeHandle":"pvc-11111111-1111-1111-1111-111111111111/3"}}}`,
			want: volume.ID{ResourceName: "pvc-11111111-1111-1111-1111-111111111111", VolumeNumber: 3},
		},
		{
			name:    "missing CSI source",
			raw:     `{"spec":{}}`,
			wantErr: true,
		},
		{
			name:    "empty volume handle",
			raw:     `{"spec":{"csi":{"volumeHandle":""}}}`,
			wantErr: true,
		},
		{
			name:    "invalid json",
			raw:     `not json`,
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := savedID(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("savedID(%q) = %v, want error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("savedID(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("savedID(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}
