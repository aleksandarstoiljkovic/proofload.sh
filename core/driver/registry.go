package driver

import (
	"fmt"
	"sort"
)

// registry holds drivers by target name. Registration happens in each engine's
// main package via an init or an explicit Register call before cli.Run.
var registry = map[string]Driver{}

// Register adds a driver to the registry. It panics on a nil driver or a
// duplicate name, since both indicate a programming error at startup.
func Register(d Driver) {
	if d == nil {
		panic("driver: Register called with nil Driver")
	}
	name := d.Name()
	if name == "" {
		panic("driver: Register called with empty Name")
	}
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("driver: duplicate registration for %q", name))
	}
	registry[name] = d
}

// Get returns the driver registered under name.
func Get(name string) (Driver, error) {
	d, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("driver: no target registered as %q (have %v)", name, Names())
	}
	return d, nil
}

// Names returns the sorted list of registered target names.
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
