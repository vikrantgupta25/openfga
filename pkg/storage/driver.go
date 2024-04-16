package storage

import "context"

type OpenFGADatastoreDriver interface {
	Open(uri string) (OpenFGADatastore, error)
}

type DriverConnector interface {
	Connect(context.Context) (OpenFGADatastore, error)

	// Driver returns the underlying OpenFGADatastoreDriver of the
	// DriverConnector
	Driver() OpenFGADatastoreDriver
}
