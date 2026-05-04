package version

import linstor "github.com/LINBIT/golinstor"

const (
	SavedPVPropKey           = linstor.NamespcAuxiliary + "/affinity-updater-saved-pv"
	SkipPropKey              = linstor.NamespcAuxiliary + "/affinity-updater-skip"
	OverrideAnnotationPrefix = "override.piraeus.io"
	SkipAnnotation           = "piraeus.io/skip-affinity-controller"
)
