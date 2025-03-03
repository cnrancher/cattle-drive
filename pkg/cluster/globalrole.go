package cluster

import (
	"strings"

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
)

type GlobalRole struct {
	Name     string
	Obj      *v3.GlobalRole
	Migrated bool
	Diff     bool
}

func newGR(obj v3.GlobalRole) *GlobalRole {
	return &GlobalRole{
		Name:     obj.Name,
		Obj:      obj.DeepCopy(),
		Migrated: false,
	}
}

// normalize will remove unneeded fields in the spec to make it easier to compare
func (g *GlobalRole) normalize() {
}

func (g *GlobalRole) Mutate() {
	g.Obj.SetName(g.Name)
	g.Obj.SetFinalizers(nil)
	g.Obj.SetResourceVersion("")
	g.Obj.SetLabels(nil)
	for annotation := range g.Obj.Annotations {
		if strings.Contains(annotation, lifeCycleAnnotationPrefix) {
			delete(g.Obj.Annotations, annotation)
		}

		if strings.Contains(annotation, cleanUpAnnotationPrefix) {
			delete(g.Obj.Annotations, annotation)
		}

		if strings.Contains(annotation, "field.cattle.io/creatorId") {
			delete(g.Obj.Annotations, annotation)
		}
	}
}
