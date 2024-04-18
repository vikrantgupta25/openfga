package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"google.golang.org/grpc"

	"github.com/openfga/openfga/pkg/middleware/registry"
	"github.com/openfga/openfga/pkg/storage"
)

// PluginManager provides a mechanism to discover and register
// OpenFGA runtime plugins.
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

func (p *PluginManager) RegisterUnaryServerInterceptors(
	interceptors ...grpc.UnaryServerInterceptor,
) error {
	registry.RegisterUnaryServerInterceptors(interceptors...)
	return nil
}
