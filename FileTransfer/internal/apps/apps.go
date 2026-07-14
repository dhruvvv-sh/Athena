// Package apps loads the application registry: a flat map of application name to its
// endpoint URL (config/applications.yml). Flows reference applications only by name; the
// registry maps those names to endpoints so a worker/master can reach the right node.
//
//	app1: https://localhost:9095
//	app2: https://localhost:9096
package apps

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// Registry maps application name -> endpoint URL.
type Registry map[string]string

// Load reads the applications YAML at path. A missing file yields an empty registry.
func Load(path string) (Registry, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Registry{}, nil
	}
	if err != nil {
		return nil, err
	}
	r := Registry{}
	if err := yaml.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return r, nil
}

// Endpoint returns the endpoint URL for app, or "" if unknown.
func (r Registry) Endpoint(app string) string { return r[app] }

// Has reports whether app is registered.
func (r Registry) Has(app string) bool { _, ok := r[app]; return ok }

// Names returns the registered application names, sorted.
func (r Registry) Names() []string {
	out := make([]string, 0, len(r))
	for k := range r {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
