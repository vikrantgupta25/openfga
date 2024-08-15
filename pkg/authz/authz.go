package authz

import (
	"context"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/logger"
)

type ctxKey string

const skipAuthz ctxKey = "skip-authz-key"

type Config struct {
	StoreID string
	ModelID string
}

type Authorizer struct {
	config *Config
	server ServerInterface
	logger logger.Logger
}

// NewAuthorizer creates a new authorizer.
func NewAuthorizer(config *Config, server ServerInterface, logger logger.Logger) (*Authorizer, error) {
	if config == nil || config.StoreID == "" || config.ModelID == "" {
		return nil, fmt.Errorf("'StoreID' and 'ModelID' configs must be set")
	}

	return &Authorizer{
		config: config,
		server: server,
		logger: logger,
	}, nil
}

// TODO: move all strings to consts here

func (a *Authorizer) getRelation(apiMethod string) (string, error) {
	switch apiMethod {
	case "ReadAuthorizationModel", "ReadAuthorizationModels":
		return "can_call_read_authorization_models", nil
	case "Read":
		return "can_call_read", nil
	case "Write":
		return "can_call_write", nil
	case "ListObjects", "StreamedListObjects":
		return "can_call_list_objects", nil
	case "Check":
		return "can_call_check", nil
	case "ListUsers":
		return "can_call_list_users", nil
	case "WriteAssertions":
		return "can_call_write_assertions", nil
	case "ReadAssertions":
		return "can_call_read_assertions", nil
	case "WriteAuthorizationModel":
		return "can_call_write_authorization_models", nil
	case "ListStores":
		return "can_call_list_stores", nil
	case "CreateStore":
		return "can_call_create_stores", nil
	case "GetStore":
		return "can_call_get_store", nil
	case "DeleteStore":
		return "can_call_delete_store", nil
	case "Expand":
		return "can_call_expand", nil
	case "ReadChanges":
		return "can_call_read_changes", nil
	default:
		return "", fmt.Errorf("unknown api method: %s", apiMethod)
	}
}

func (a *Authorizer) AuthorizeCreateStore(ctx context.Context, clientID string) (bool, error) {
	relation, err := a.getRelation("CreateStore")
	if err != nil {
		return false, err
	}
	allowed, err := a.individualAuthorize(ctx, clientID, relation, a.getSystem(), &openfgav1.ContextualTupleKeys{})
	if !allowed || err != nil {
		return false, err
	}

	return true, nil
}

func (a *Authorizer) ListAuthorizedStores(ctx context.Context, clientID string) ([]string, error) {
	relation, err := a.getRelation("ListStores")
	if err != nil {
		return nil, err
	}
	req := &openfgav1.ListObjectsRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		User:                 a.getApplication(clientID),
		Relation:             relation,
		Type:                 "store",
	}

	resp, err := a.server.ListObjectsWithoutAuthz(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetObjects(), nil
}

// ContextWithSkipAuthzCheck attaches whether to skip authz check to the parent context.
func ContextWithSkipAuthzCheck(parent context.Context, skipAuthzCheck bool) context.Context {
	return context.WithValue(parent, skipAuthz, skipAuthzCheck)
}

// SkipAuthzCheckFromContext returns whether the authorize check can be skipped.
func SkipAuthzCheckFromContext(ctx context.Context) bool {
	isSkipped, ok := ctx.Value(skipAuthz).(bool)
	return isSkipped && ok
}

// Authorize checks if the user has access to the resource.
func (a *Authorizer) Authorize(ctx context.Context, clientID, storeID, apiMethod string, modules ...string) (bool, error) {
	if SkipAuthzCheckFromContext(ctx) {
		return true, nil
	}
	relation, err := a.getRelation(apiMethod)
	if err != nil {
		return false, err
	}

	if len(modules) > 0 {
		return a.moduleAuthorize(ctx, clientID, relation, storeID, modules)
	}

	return a.individualAuthorize(ctx, clientID, relation, a.getStore(storeID), &openfgav1.ContextualTupleKeys{})
}

func (a *Authorizer) individualAuthorize(ctx context.Context, clientID, relation, object string, contextualTuples *openfgav1.ContextualTupleKeys) (bool, error) {
	req := &openfgav1.CheckRequest{
		StoreId:              a.config.StoreID,
		AuthorizationModelId: a.config.ModelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     a.getApplication(clientID),
			Relation: relation,
			Object:   object,
		},
		ContextualTuples: contextualTuples,
	}

	resp, err := a.server.CheckWithoutAuthz(ctx, req)
	if err != nil {
		return false, err
	}

	if !resp.GetAllowed() {
		return false, nil
	}

	return true, nil
}

func (a *Authorizer) moduleAuthorize(ctx context.Context, clientID, relation, storeID string, modules []string) (bool, error) {
	var err error
	var wg sync.WaitGroup
	errorChannel := make(chan error, len(modules))
	defer close(errorChannel)
	done := make(chan struct{})
	defer close(done)

	for _, module := range modules {
		wg.Add(1)
		go func(module string) {
			defer wg.Done()
			contextualTuples := openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     a.getStore(storeID),
						Relation: "store",
						Object:   a.getModule(storeID, module),
					},
				},
			}

			allowed, err := a.individualAuthorize(ctx, clientID, relation, a.getModule(storeID, module), &contextualTuples)

			if err != nil || !allowed {
				errorChannel <- err
			}
		}(module)
	}

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case err = <-errorChannel:
		return false, err
	case <-done:
		return true, nil
	}
}

func (a *Authorizer) getStore(storeID string) string {
	return fmt.Sprintf(`store:%s`, storeID)
}

func (a *Authorizer) getApplication(clientID string) string {
	return fmt.Sprintf(`application:%s`, clientID)
}

func (a *Authorizer) getModule(storeID, module string) string {
	return fmt.Sprintf(`module:%s|%s`, storeID, module)
}

func (a *Authorizer) getSystem() string {
	return fmt.Sprintf(`system:%s`, "fga")
}
