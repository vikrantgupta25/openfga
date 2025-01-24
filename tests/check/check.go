// Package check contains integration tests for the query APIs (ListObjects, ListUsers and Check)
package check

import (
	"context"
	"fmt"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"go.uber.org/goleak"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/assets"
	checktest "github.com/openfga/openfga/internal/test/check"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

var writeMaxChunkSize = 40 // chunk write requests into a chunks of this max size

type individualTest struct {
	Name   string
	Stages []*stage
}

type checkTests struct {
	Tests []individualTest
}

type testParams struct {
	schemaVersion string
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Name            string // optional
	Model           string
	Tuples          []*openfgav1.TupleKey
	CheckAssertions []*checktest.Assertion `json:"checkAssertions"`
}

// ClientInterface defines client interface for running check tests.
type ClientInterface interface {
	tests.TestClientBootstrapper
	Check(ctx context.Context, in *openfgav1.CheckRequest, opts ...grpc.CallOption) (*openfgav1.CheckResponse, error)
	ListUsers(ctx context.Context, in *openfgav1.ListUsersRequest, opts ...grpc.CallOption) (*openfgav1.ListUsersResponse, error)
	ListObjects(ctx context.Context, in *openfgav1.ListObjectsRequest, opts ...grpc.CallOption) (*openfgav1.ListObjectsResponse, error)
}

// RunAllTests will run all check tests.
func RunAllTests(t *testing.T, engine string) {
	files := []string{
		"tests/abac_tests.yaml",
	}

	var allTestCases []individualTest

	for _, file := range files {
		var b []byte
		var err error
		b, err = assets.EmbedTests.ReadFile(file)
		require.NoError(t, err)

		var testCases checkTests
		err = yaml.Unmarshal(b, &testCases)
		require.NoError(t, err)

		allTestCases = append(allTestCases, testCases.Tests...)
	}

	for _, test := range allTestCases {
		test := test
		runTest(t, test, engine, false)
		runTest(t, test, engine, true)
	}
}

func runTest(t *testing.T, test individualTest, engine string, contextTupleTest bool) {
	name := test.Name

	if contextTupleTest {
		name += "_ctxTuples"
	}

	t.Run(name, func(t *testing.T) {

		for stageNumber, stage := range test.Stages {
			t.Run(fmt.Sprintf("stage_%d", stageNumber), func(t *testing.T) {
				if contextTupleTest && len(test.Stages) > 1 {
					// we don't want to run special contextual tuples test for these cases
					// as multi-stages test has expectation tuples are in system
					t.Skipf("multi-stages test has expectation tuples are in system")
				}
				t.Cleanup(func() {
					goleak.VerifyNone(t)
				})
				container := storage.RunDatastoreTestContainer(t, engine)
				cfg := config.MustDefaultConfig()
				cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations")
				cfg.Log.Level = "error"
				cfg.Datastore.URI = container.GetConnectionURI(true)
				cfg.Datastore.Engine = "mysql"
				// extend the timeout for the tests, coverage makes them slower
				cfg.RequestTimeout = 10 * time.Second

				cfg.CheckIteratorCache.Enabled = true

				// Some tests/stages are sensitive to the cache TTL,
				// so we set it to a very low value to still exercise
				// the Check iterator cache.
				cfg.CheckIteratorCache.TTL = 1 * time.Nanosecond

				tests.StartServer(t, cfg)

				conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)
				client := openfgav1.NewOpenFGAServiceClient(conn)

				ctx := context.Background()

				resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: name})
				require.NoError(t, err)

				storeID := resp.GetId()

				if contextTupleTest && len(stage.Tuples) > 100 {
					// https://github.com/openfga/api/blob/6e048d8023f434cb7a1d3943f41bdc3937d4a1bf/openfga/v1/openfga.proto#L222
					t.Skipf("cannot send more than 100 contextual tuples in one request")
				}
				// arrange: write model
				model := testutils.MustTransformDSLToProtoWithID(stage.Model)

				writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   typesystem.SchemaVersion1_1,
					TypeDefinitions: model.GetTypeDefinitions(),
					Conditions:      model.GetConditions(),
				})
				require.NoError(t, err)

				tuples := testutils.Shuffle(stage.Tuples)
				tuplesLength := len(tuples)
				// arrange: write tuples
				if tuplesLength > 0 && !contextTupleTest {
					for i := 0; i < tuplesLength; i += writeMaxChunkSize {
						end := int(math.Min(float64(i+writeMaxChunkSize), float64(tuplesLength)))
						writeChunk := (tuples)[i:end]
						_, err = client.Write(ctx, &openfgav1.WriteRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							Writes: &openfgav1.WriteRequestWrites{
								TupleKeys: writeChunk,
							},
						})
						require.NoError(t, err)
					}
				}

				if len(stage.CheckAssertions) == 0 {
					t.Skipf("no check assertions defined")
				}
				for assertionNumber, assertion := range stage.CheckAssertions {
					t.Run(fmt.Sprintf("assertion_%d", assertionNumber), func(t *testing.T) {
						detailedInfo := fmt.Sprintf("Check request: %s. Model: %s. Tuples: %s. Contextual tuples: %s", assertion.Tuple, stage.Model, stage.Tuples, assertion.ContextualTuples)

						ctxTuples := testutils.Shuffle(assertion.ContextualTuples)
						if contextTupleTest {
							ctxTuples = append(ctxTuples, stage.Tuples...)
						}

						var tupleKey *openfgav1.CheckRequestTupleKey
						if assertion.Tuple != nil {
							tupleKey = &openfgav1.CheckRequestTupleKey{
								User:     assertion.Tuple.GetUser(),
								Relation: assertion.Tuple.GetRelation(),
								Object:   assertion.Tuple.GetObject(),
							}
						}
						resp, err := client.Check(ctx, &openfgav1.CheckRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							TupleKey:             tupleKey,
							ContextualTuples: &openfgav1.ContextualTupleKeys{
								TupleKeys: ctxTuples,
							},
							Context: assertion.Context,
							Trace:   true,
						})

						if assertion.ErrorCode == 0 {
							require.NoError(t, err, detailedInfo)
							require.Equal(t, assertion.Expectation, resp.GetAllowed(), detailedInfo)
						} else {
							require.Error(t, err, detailedInfo)
							e, ok := status.FromError(err)
							require.True(t, ok, detailedInfo)
							require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
						}
					})
				}
			})
		}
	})
}
