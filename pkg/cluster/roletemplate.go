package cluster

import (
	"strings"

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
)

type RoleTemplate struct {
	Name     string
	Obj      *v3.RoleTemplate
	Migrated bool
	Diff     bool
}

func newRoleTemplate(obj v3.RoleTemplate) *RoleTemplate {
	return &RoleTemplate{
		Name:     obj.Name,
		Obj:      obj.DeepCopy(),
		Migrated: false,
	}
}

// normalize will remove unneeded fields in the spec to make it easier to compare
func (r *RoleTemplate) normalize() {
}

func (r *RoleTemplate) Mutate() {
	r.Obj.SetName(r.Name)
	r.Obj.SetFinalizers(nil)
	r.Obj.SetResourceVersion("")
	r.Obj.SetLabels(nil)
	for annotation := range r.Obj.Annotations {
		if strings.Contains(annotation, lifeCycleAnnotationPrefix) {
			delete(r.Obj.Annotations, annotation)
		}

		if strings.Contains(annotation, cleanUpAnnotationPrefix) {
			delete(r.Obj.Annotations, annotation)
		}

		if strings.Contains(annotation, "field.cattle.io/creatorId") {
			delete(r.Obj.Annotations, annotation)
		}
	}
}
