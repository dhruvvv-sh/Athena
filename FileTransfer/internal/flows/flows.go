// Package flows loads the per-flow definitions from <Home>/flows/*.yml. There is one
// file per flow, named by the flow identifier, and each file describes BOTH ends:
//
//	flow_identifier: app1_app2_biz_in     # <Sender>_<Receiver>_<Functionality>_<Country>
//	sender:
//	  application: app1
//	  file_path:   tmp/outbox             # sandbox, relative to the sender app's home
//	  cn:          app1-cert              # client-cert CN that authenticates the sender
//	receiver:
//	  application: app2
//	  file_path:   tmp/inbox              # sandbox, relative to the receiver app's home
//	  cn:          app2-cert
//
// A sender may only READ files under its file_path; a receiver may only have files
// WRITTEN under its file_path. A relative file_path is resolved under the application's
// deployment home — by convention a sibling directory named after the application under
// the deployments' parent (e.g. .../deploy/app2/tmp/inbox for application "app2").
package flows

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	Sender   = "sender"
	Receiver = "receiver"
)

// Folder operations gated by an endpoint's permissions.
const (
	PermRead   = "read"
	PermWrite  = "write"
	PermList   = "list"
	PermDelete = "delete"
)

var validPerms = map[string]bool{PermRead: true, PermWrite: true, PermList: true, PermDelete: true}

// Endpoint is one side (sender or receiver) of a flow.
type Endpoint struct {
	Application string `yaml:"application"`
	FilePath    string `yaml:"file_path"` // resolved to an absolute sandbox path at load
	CN          string `yaml:"cn"`
	Permissions string `yaml:"permissions"` // comma-separated: read, write, list, delete
	perms       map[string]bool
}

// Can reports whether this endpoint is permitted the operation (read/write/list/delete).
// A missing permission means the operation is denied.
func (e *Endpoint) Can(op string) bool { return e.perms[op] }

// Perms returns the granted permissions in canonical order (for the API/UI).
func (e *Endpoint) Perms() []string {
	out := []string{}
	for _, p := range []string{PermRead, PermWrite, PermList, PermDelete} {
		if e.perms[p] {
			out = append(out, p)
		}
	}
	return out
}

// parsePermissions turns "read, write, list" into a validated set.
func parsePermissions(s string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, tok := range strings.Split(s, ",") {
		t := strings.ToLower(strings.TrimSpace(tok))
		if t == "" {
			continue
		}
		if !validPerms[t] {
			return nil, fmt.Errorf("invalid permission %q (want read/write/list/delete)", t)
		}
		out[t] = true
	}
	return out, nil
}

// Flow is a sender->receiver relationship identified by Identifier.
type Flow struct {
	Identifier string   `yaml:"flow_identifier"`
	Sender     Endpoint `yaml:"sender"`
	Receiver   Endpoint `yaml:"receiver"`
	sourceFile string
}

type Set struct {
	all  []*Flow
	byID map[string]*Flow
}

// Load reads every *.yml / *.yaml under dir. Missing dir => empty set. `parent` is the
// deployments' parent directory: a relative file_path for application "X" resolves to
// <parent>/X/<file_path>.
func Load(dir, parent string) (*Set, error) {
	s := &Set{byID: map[string]*Flow{}}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".yml" && ext != ".yaml" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var f Flow
		if err := yaml.Unmarshal(b, &f); err != nil {
			return nil, fmt.Errorf("flow %s: %w", e.Name(), err)
		}
		f.sourceFile = e.Name()
		if err := f.validate(); err != nil {
			return nil, fmt.Errorf("flow %s: %w", e.Name(), err)
		}
		f.Sender.FilePath = resolveBase(parent, f.Sender.Application, f.Sender.FilePath)
		f.Receiver.FilePath = resolveBase(parent, f.Receiver.Application, f.Receiver.FilePath)
		if f.Sender.perms, err = parsePermissions(f.Sender.Permissions); err != nil {
			return nil, fmt.Errorf("flow %s: sender: %w", e.Name(), err)
		}
		if f.Receiver.perms, err = parsePermissions(f.Receiver.Permissions); err != nil {
			return nil, fmt.Errorf("flow %s: receiver: %w", e.Name(), err)
		}
		if prev, ok := s.byID[f.Identifier]; ok {
			return nil, fmt.Errorf("flow %s: identifier %q already used by %s", e.Name(), f.Identifier, prev.sourceFile)
		}
		fp := f
		s.all = append(s.all, &fp)
		s.byID[f.Identifier] = &fp
	}
	sort.Slice(s.all, func(i, j int) bool { return s.all[i].Identifier < s.all[j].Identifier })
	return s, nil
}

// resolveBase makes an endpoint's file_path absolute: an absolute path is used as-is; a
// relative one is placed under <parent>/<application>/.
func resolveBase(parent, app, fp string) string {
	if fp == "" {
		return ""
	}
	if filepath.IsAbs(fp) {
		return filepath.Clean(fp)
	}
	return filepath.Clean(filepath.Join(parent, app, fp))
}

func (f *Flow) validate() error {
	if f.Identifier == "" {
		return fmt.Errorf("flow_identifier is required")
	}
	for _, e := range []struct {
		role string
		ep   Endpoint
	}{{"sender", f.Sender}, {"receiver", f.Receiver}} {
		if e.ep.Application == "" {
			return fmt.Errorf("%s.application is required", e.role)
		}
		if e.ep.FilePath == "" {
			return fmt.Errorf("%s.file_path is required", e.role)
		}
		if e.ep.CN == "" {
			return fmt.Errorf("%s.cn is required", e.role)
		}
	}
	return nil
}

// Count reports how many flows were loaded.
func (s *Set) Count() int { return len(s.all) }

// All returns the loaded flows (ordered by identifier).
func (s *Set) All() []*Flow { return s.all }

// ByID returns the flow with the given identifier, or nil.
func (s *Set) ByID(id string) *Flow { return s.byID[id] }

// Apps returns the set of application names referenced by any flow (sender or receiver).
func (s *Set) Apps() []string {
	seen := map[string]bool{}
	for _, f := range s.all {
		seen[f.Sender.Application] = true
		seen[f.Receiver.Application] = true
	}
	out := make([]string, 0, len(seen))
	for a := range seen {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// Match finds a flow by its sender (by client-cert CN, or by application name) and
// receiver application. senderCN takes precedence when set. Returns nil if none match.
func (s *Set) Match(senderCN, senderApp, receiverApp string) *Flow {
	for _, f := range s.all {
		if receiverApp != "" && !strings.EqualFold(f.Receiver.Application, receiverApp) {
			continue
		}
		if senderCN != "" && f.Sender.CN == senderCN {
			return f
		}
		if senderCN == "" && senderApp != "" && strings.EqualFold(f.Sender.Application, senderApp) {
			return f
		}
	}
	return nil
}

// EndpointFor returns the sender or receiver endpoint by role ("sender"/"receiver").
func (f *Flow) EndpointFor(role string) *Endpoint {
	if strings.EqualFold(role, Receiver) {
		return &f.Receiver
	}
	return &f.Sender
}

// ── API/UI projections ──

type EndpointView struct {
	Application string   `json:"application"`
	Path        string   `json:"path"`
	CN          string   `json:"cn"`
	Permissions []string `json:"permissions"`
}

type View struct {
	Identifier string       `json:"flow_identifier"`
	Sender     EndpointView `json:"sender"`
	Receiver   EndpointView `json:"receiver"`
}

// List returns every flow as a read-only view (ordered by identifier).
func (s *Set) List() []View {
	out := make([]View, 0, len(s.all))
	for _, f := range s.all {
		out = append(out, View{
			Identifier: f.Identifier,
			Sender:     EndpointView{f.Sender.Application, f.Sender.FilePath, f.Sender.CN, f.Sender.Perms()},
			Receiver:   EndpointView{f.Receiver.Application, f.Receiver.FilePath, f.Receiver.CN, f.Receiver.Perms()},
		})
	}
	return out
}

// ── sandbox browsing (per endpoint) ──

// Entry is one file or subdirectory inside a sandbox.
type Entry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

// Browse lists the immediate entries under rel (relative to the endpoint sandbox);
// rel=="" lists the sandbox root. Directories first, then files, alphabetically. A
// missing sandbox dir yields an empty listing; traversal is rejected via Resolve.
func (e *Endpoint) Browse(rel string) (string, []Entry, error) {
	dir := filepath.Clean(e.FilePath)
	if r := strings.TrimSpace(rel); r != "" && r != "." {
		abs, err := e.Resolve(r)
		if err != nil {
			return "", nil, err
		}
		dir = abs
	}
	des, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return dir, []Entry{}, nil
	}
	if err != nil {
		return "", nil, err
	}
	out := make([]Entry, 0, len(des))
	for _, de := range des {
		en := Entry{Name: de.Name(), IsDir: de.IsDir()}
		if info, ierr := de.Info(); ierr == nil {
			en.Size = info.Size()
		}
		out = append(out, en)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir
		}
		return out[i].Name < out[j].Name
	})
	return dir, out, nil
}

// Resolve joins rel to the endpoint's file_path and rejects any path that would escape
// the sandbox — absolute inputs and ".." traversal are refused.
func (e *Endpoint) Resolve(rel string) (string, error) {
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return "", fmt.Errorf("path is empty")
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("path %q must be relative to the sandbox", rel)
	}
	base := filepath.Clean(e.FilePath)
	abs := filepath.Clean(filepath.Join(base, rel))
	if abs != base && !strings.HasPrefix(abs, base+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes the sandbox %q", rel, e.FilePath)
	}
	return abs, nil
}

// Contains reports whether abs is inside this endpoint's sandbox. It accepts only
// absolute paths because remote receivers validate the already-resolved target path.
func (e *Endpoint) Contains(abs string) bool {
	if !filepath.IsAbs(abs) {
		return false
	}
	base := filepath.Clean(e.FilePath)
	clean := filepath.Clean(abs)
	return clean == base || strings.HasPrefix(clean, base+string(os.PathSeparator))
}
