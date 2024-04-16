package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"github.com/openfga/openfga/pkg/storage"
)

// PluginManager needs to provide a way to:
// 1. discover plugins (datastore, middleware, etc..)
// 2. register the middleware so the main app is aware of it
type PluginManager struct {
	datastoreRegisterOnce sync.Once
}

func LoadPlugins(path string) (*PluginManager, error) {
	c, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	pm := &PluginManager{}

	for _, entry := range c {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".so") {
			fullpath := filepath.Join(path, entry.Name())
			fmt.Println("found plugin file", fullpath)

			p, err := plugin.Open(fullpath)
			if err != nil {
				return nil, err
			}

			ifunc, err := p.Lookup("InitPlugin")
			if err != nil {
				return nil, err
			}

			initFunc := ifunc.(func(*PluginManager) error)
			if err := initFunc(pm); err != nil {
				return nil, err
			}
		}
	}

	return pm, nil
}

func (p *PluginManager) RegisterOpenFGADatastore(
	engine string,
	driver storage.OpenFGADatastoreDriver,
) error {
	p.datastoreRegisterOnce.Do(func() {
		storage.Register(engine, driver)
	})

	return nil
}
