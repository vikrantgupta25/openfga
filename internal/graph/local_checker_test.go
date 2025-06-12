package graph

import (
	"context"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"

	"github.com/openfga/language/pkg/go/transformer"
)

// Helper to parse a model string into a TypeSystem
func parseTypeSystem(t *testing.T, model string) *typesystem.TypeSystem {
	ts, err := typesystem.NewAndValidate(context.Background(), testParseModel(t, model))
	require.NoError(t, err)
	return ts
}

// Helper to parse a model string into an AuthorizationModel
func testParseModel(t *testing.T, model string) *openfgav1.AuthorizationModel {
	jsonString, err := transformer.TransformDSLToJSON(model)
	require.NoError(t, err)
	var authModel openfgav1.AuthorizationModel
	err = protojson.Unmarshal([]byte(jsonString), &authModel)
	require.NoError(t, err)
	return &authModel
}

// Helper to create a LocalChecker with tuples and model
func newLocalChecker(t *testing.T, model string, tuples ...*openfgav1.TupleKey) (context.Context, *LocalChecker) {
	ts := parseTypeSystem(t, model)
	memstore := memory.New()
	err := memstore.Write(context.Background(), "A", nil, tuples)
	require.NoError(t, err)
	ctx := context.Background()
	ctx = typesystem.ContextWithTypesystem(ctx, ts)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, memstore)
	return ctx, NewLocalChecker()
}

// Helper to call ResolveCheck
func checkAllowed(t *testing.T, ctx context.Context, checker *LocalChecker, object, relation, user string, want bool) {
	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		TupleKey: &openfgav1.TupleKey{
			Object:   object,
			Relation: relation,
			User:     user,
		},
		StoreID:         "A",
		RequestMetadata: NewCheckRequestMetadata(),
	})
	require.NoError(t, err)
	require.Equal(t, want, resp.Allowed)
}

func TestLocalChecker_Direct(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "viewer", "user:jon"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jon", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jane", false)
}

func TestLocalChecker_Indirect(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [group#member]
type group
  relations
    define member: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("group:eng", "member", "user:jon"),
		tuple.NewTupleKey("doc:1", "viewer", "group:eng#member"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jon", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jane", false)
}

func TestLocalChecker_Union(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user, group#member]
type group
  relations
    define member: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "viewer", "user:alice"),
		tuple.NewTupleKey("doc:1", "viewer", "group:eng#member"),
		tuple.NewTupleKey("group:eng", "member", "user:bob"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:alice", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:bob", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:carol", false)
}

func TestLocalChecker_Intersection(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define editor: [user]
    define admin: [user]
    define superuser: editor and admin
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "editor", "user:jon"),
		tuple.NewTupleKey("doc:1", "admin", "user:jon"),
		tuple.NewTupleKey("doc:1", "editor", "user:jane"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "superuser", "user:jon", true)
	checkAllowed(t, ctx, checker, "doc:1", "superuser", "user:jane", false)
}

func TestLocalChecker_Exclusion(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user]
    define banned: [user]
    define allowed: viewer but not banned
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "viewer", "user:jon"),
		tuple.NewTupleKey("doc:1", "banned", "user:jon"),
		tuple.NewTupleKey("doc:1", "viewer", "user:jane"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "allowed", "user:jon", false)
	checkAllowed(t, ctx, checker, "doc:1", "allowed", "user:jane", true)
}

func TestLocalChecker_RecursiveWithExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type folder
  relations
    define parent: [folder]
    define banned: [user]
    define viewer: [user] or viewer from parent
    define allowed: viewer but not banned
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("folder:root", "viewer", "user:alice"),
		tuple.NewTupleKey("folder:child", "parent", "folder:root"),
		tuple.NewTupleKey("folder:child", "banned", "user:alice"),
	)
	checkAllowed(t, ctx, checker, "folder:child", "allowed", "user:alice", false) // banned in child
	checkAllowed(t, ctx, checker, "folder:child", "viewer", "user:alice", true)   // inherited from root
}

func TestLocalChecker_EmptyTuples(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user]
type user
`
	ctx, checker := newLocalChecker(t, model)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jon", false)
}

func TestLocalChecker_WildcardAndDirect(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user, user:*]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "viewer", "user:*"),
		tuple.NewTupleKey("doc:1", "viewer", "user:jon"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jon", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:jane", true)
}

func TestLocalChecker_ExclusionWithNoBanned(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user]
    define banned: [user]
    define allowed: viewer but not banned
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "viewer", "user:jon"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "allowed", "user:jon", true)
}

func TestLocalChecker_IntersectionWithWildcard(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define editor: [user:*]
    define admin: [user]
    define superuser: editor and admin
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "editor", "user:*"),
		tuple.NewTupleKey("doc:1", "admin", "user:jon"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "superuser", "user:jon", true)
	checkAllowed(t, ctx, checker, "doc:1", "superuser", "user:jane", false)
}

func TestLocalChecker_DeeplyNestedTupleset(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [team#member]
type team
  relations
    define member: [department#member]
type department
  relations
    define member: [division#member]
type division
  relations
    define member: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("division:div1", "member", "user:alice"),
		tuple.NewTupleKey("department:dep1", "member", "division:div1#member"),
		tuple.NewTupleKey("team:team1", "member", "department:dep1#member"),
		tuple.NewTupleKey("doc:doc1", "viewer", "team:team1#member"),
	)
	checkAllowed(t, ctx, checker, "doc:doc1", "viewer", "user:alice", true)
	checkAllowed(t, ctx, checker, "doc:doc1", "viewer", "user:bob", false)
}

func TestLocalChecker_ComplexUnionIntersectionExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define editor: [user]
    define admin: [user]
    define banned: [user]
    define privileged: editor and admin
    define allowed: privileged but not banned
    define viewer: [group#member] or allowed
type group
  relations
    define member: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "editor", "user:alice"),
		tuple.NewTupleKey("doc:1", "admin", "user:alice"),
		tuple.NewTupleKey("doc:1", "banned", "user:bob"),
		tuple.NewTupleKey("group:eng", "member", "user:carol"),
		tuple.NewTupleKey("doc:1", "viewer", "group:eng#member"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:alice", true) // editor & admin, not banned
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:bob", false)  // not editor & admin, and banned
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:carol", true) // via group membership
}

func TestLocalChecker_IndirectWildcard(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [group#member]
type group
  relations
    define member: [user:*]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("group:everyone", "member", "user:*"),
		tuple.NewTupleKey("doc:1", "viewer", "group:everyone#member"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:alice", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:bob", true)
}

func TestLocalChecker_ChainedTupleToUsersetWithExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type resource
  relations
    define shared: [project#member]
    define banned: [user]
    define allowed: shared but not banned
type project
  relations
    define member: [team#member]
type team
  relations
    define member: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("team:alpha", "member", "user:alice"),
		tuple.NewTupleKey("project:proj1", "member", "team:alpha#member"),
		tuple.NewTupleKey("resource:res1", "shared", "project:proj1#member"),
		tuple.NewTupleKey("resource:res1", "banned", "user:alice"),
	)
	checkAllowed(t, ctx, checker, "resource:res1", "allowed", "user:alice", false)
	checkAllowed(t, ctx, checker, "resource:res1", "shared", "user:alice", true)
}

func TestLocalChecker_UnionOfWildcardsAndDirect(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [user, user:*]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "viewer", "user:*"),
		tuple.NewTupleKey("doc:1", "viewer", "user:alice"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:alice", true)
	checkAllowed(t, ctx, checker, "doc:1", "viewer", "user:bob", true)
}

func TestLocalChecker_IntersectionOfWildcards(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define editor: [user:*]
    define admin: [user:*]
    define superuser: editor and admin
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("doc:1", "editor", "user:*"),
		tuple.NewTupleKey("doc:1", "admin", "user:*"),
	)
	checkAllowed(t, ctx, checker, "doc:1", "superuser", "user:alice", true)
	checkAllowed(t, ctx, checker, "doc:1", "superuser", "user:bob", true)
}

func TestLocalChecker_MultiPathTupleToUsersetWithCyclesAndExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define viewer: [group#member, team#member]
    define banned: [user]
    define allowed: viewer but not banned
type group
  relations
    define member: [user, team#member]
type team
  relations
    define member: [user, group#member]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("group:g1", "member", "user:alice"),
		tuple.NewTupleKey("team:t1", "member", "group:g1#member"),
		tuple.NewTupleKey("group:g1", "member", "team:t1#member"), // cycle
		tuple.NewTupleKey("doc:d1", "viewer", "group:g1#member"),
		tuple.NewTupleKey("doc:d1", "viewer", "team:t1#member"),
		tuple.NewTupleKey("doc:d1", "banned", "user:alice"),
	)
	// alice is in a cycle, but should not infinite loop, and is banned
	checkAllowed(t, ctx, checker, "doc:d1", "allowed", "user:alice", false)
	// bob is not a member anywhere
	checkAllowed(t, ctx, checker, "doc:d1", "allowed", "user:bob", false)
}

func TestLocalChecker_ComplexMultiLevelUnionIntersectionExclusionWildcard(t *testing.T) {
	model := `
model
  schema 1.1
type asset
  relations
    define editor: [user, user:*]
    define admin: [user]
    define banned: [user]
    define privileged: editor and admin
    define allowed: privileged but not banned
    define viewer: [group#member, team#member] or allowed
type group
  relations
    define member: [user, user:*]
type team
  relations
    define member: [user]
type user
`
	ctx, checker := newLocalChecker(t, model,
		tuple.NewTupleKey("asset:a1", "editor", "user:*"),
		tuple.NewTupleKey("asset:a1", "admin", "user:alice"),
		tuple.NewTupleKey("asset:a1", "banned", "user:bob"),
		tuple.NewTupleKey("group:g1", "member", "user:carol"),
		tuple.NewTupleKey("group:g1", "member", "user:*"),
		tuple.NewTupleKey("team:t1", "member", "user:dave"),
		tuple.NewTupleKey("asset:a1", "viewer", "group:g1#member"),
		tuple.NewTupleKey("asset:a1", "viewer", "team:t1#member"),
	)
	// alice: editor (via wildcard) and admin, not banned
	checkAllowed(t, ctx, checker, "asset:a1", "viewer", "user:alice", true)
	// bob: editor (via wildcard), not admin, and banned
	checkAllowed(t, ctx, checker, "asset:a1", "viewer", "user:bob", false)
	// carol: via group membership
	checkAllowed(t, ctx, checker, "asset:a1", "viewer", "user:carol", true)
	// dave: via team membership
	checkAllowed(t, ctx, checker, "asset:a1", "viewer", "user:dave", true)
	// eve: via group wildcard
	checkAllowed(t, ctx, checker, "asset:a1", "viewer", "user:eve", true)
}

func TestLocalChecker_DeepAndWideRecursion(t *testing.T) {
	model := `
model
  schema 1.1
type resource
  relations
    define parent: [resource]
    define sibling: [resource]
    define editor: [user] or editor from parent or editor from sibling
    define admin: [user] or admin from parent or admin from sibling
    define banned: [user] or banned from parent or banned from sibling
    define privileged: editor and admin
    define allowed: privileged but not banned
    define viewer: [user, group#member, team#member] or allowed or viewer from parent or viewer from sibling
type group
  relations
    define member: [user, user:*]
type team
  relations
    define member: [user, group#member]
type user
`
	// Deep chain: resource:root -> resource:l1 -> resource:l2 -> ... -> resource:l10
	// Wide siblings at each level, and group/team memberships
	tuples := []*openfgav1.TupleKey{}
	depth := 10
	breadth := 5

	// Create users for each depth and breadth
	for d := 0; d <= depth; d++ {
		for b := 0; b < breadth; b++ {
			uid := fmt.Sprintf("user:d%02db%02d", d, b)
			rid := fmt.Sprintf("resource:l%ds%d", d, b)
			// Each resource at each level and breadth has an editor, admin, and banned user
			tuples = append(tuples,
				tuple.NewTupleKey(rid, "editor", uid),
				tuple.NewTupleKey(rid, "admin", uid),
				tuple.NewTupleKey(rid, "banned", "user:evil"),
			)
			// Each resource at each level and breadth has a group and team
			gid := fmt.Sprintf("group:g%ds%d", d, b)
			tid := fmt.Sprintf("team:t%ds%d", d, b)
			tuples = append(tuples,
				tuple.NewTupleKey(gid, "member", uid),
				tuple.NewTupleKey(tid, "member", uid),
				tuple.NewTupleKey(rid, "viewer", gid+"#member"),
				tuple.NewTupleKey(rid, "viewer", tid+"#member"),
			)
			// Sibling relationships (wide)
			if b > 0 {
				prevRid := fmt.Sprintf("resource:l%ds%d", d, b-1)
				tuples = append(tuples, tuple.NewTupleKey(rid, "sibling", prevRid))
			}
			// Parent relationships (deep)
			if d > 0 {
				parentRid := fmt.Sprintf("resource:l%ds%d", d-1, b)
				tuples = append(tuples, tuple.NewTupleKey(rid, "parent", parentRid))
			}
		}
	}

	// Add a wildcard group at the deepest level
	tuples = append(tuples,
		tuple.NewTupleKey("group:gwild", "member", "user:*"),
		tuple.NewTupleKey("resource:l10s0", "viewer", "group:gwild#member"),
	)

	ctx, checker := newLocalChecker(t, model, tuples...)

	// Test a user at the deepest level, breadth 0
	checkAllowed(t, ctx, checker, "resource:l10s0", "viewer", "user:d10b00", true) // not present, but wildcard group
	checkAllowed(t, ctx, checker, "resource:l10s0", "viewer", "user:evil", false)  // banned everywhere

	// Test a user who is editor and admin at every level and breadth
	for d := 0; d <= depth; d++ {
		for b := 0; b < breadth; b++ {
			uid := fmt.Sprintf("user:d%02db%02d", d, b)
			rid := fmt.Sprintf("resource:l%ds%d", d, b)
			checkAllowed(t, ctx, checker, rid, "allowed", uid, true)
			checkAllowed(t, ctx, checker, rid, "viewer", uid, true)
		}
	}
}

func TestLocalChecker_DeepRecursionWithTTUAndExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type node
  relations
    define parent: [node]
    define banned: [user] or banned from parent
    define ancestor: [node]
    define viewer: [user] or viewer from ancestor
    define allowed: viewer but not banned
type user
`
	// Build a chain of 20 nodes, with user:alice as viewer at the root, and user:bob banned at node:10
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("node:root", "viewer", "user:alice"),
	}
	for i := 1; i <= 20; i++ {
		child := fmt.Sprintf("node:n%02d", i)
		parent := "node:root"
		if i > 1 {
			parent = fmt.Sprintf("node:n%02d", i-1)
		}
		tuples = append(tuples, tuple.NewTupleKey(child, "parent", parent))
		// Each node's ancestor is its parent
		tuples = append(tuples, tuple.NewTupleKey(child, "ancestor", parent))
	}
	tuples = append(tuples, tuple.NewTupleKey("node:n10", "banned", "user:bob"))

	ctx, checker := newLocalChecker(t, model, tuples...)

	// alice should be allowed at the deepest node
	checkAllowed(t, ctx, checker, "node:n20", "allowed", "user:alice", true)
	// bob should not be allowed at or below node:n10
	checkAllowed(t, ctx, checker, "node:n10", "allowed", "user:bob", false)
	checkAllowed(t, ctx, checker, "node:n20", "allowed", "user:bob", false)
	// carol should not be allowed anywhere
	checkAllowed(t, ctx, checker, "node:n20", "allowed", "user:carol", false)
}

func TestLocalChecker_MultiTypeDeepBreadthRecursionWithTTUAndExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type resource
  relations
    define parent: [resource]
    define related: [resource]
    define editor: [user] or editor from parent or editor from related
    define admin: [user] or admin from parent or admin from related
    define banned: [user] or banned from parent or banned from related
    define privileged: editor and admin
    define allowed: privileged but not banned
    define viewer: [user, group#member, team#member] or allowed or viewer from parent or viewer from related
type group
  relations
    define member: [user, user:*]
type team
  relations
    define member: [user, group#member]
type user
`
	// Build a deep and wide graph with cross-links and exclusions
	tuples := []*openfgav1.TupleKey{}
	depth := 6
	breadth := 4

	// Create users, groups, teams, and resources with cross-links
	for d := 0; d <= depth; d++ {
		for b := 0; b < breadth; b++ {
			uid := fmt.Sprintf("user:d%db%d", d, b)
			rid := fmt.Sprintf("resource:l%ds%d", d, b)
			gid := fmt.Sprintf("group:g%ds%d", d, b)
			tid := fmt.Sprintf("team:t%ds%d", d, b)

			// Direct assignments
			tuples = append(tuples,
				tuple.NewTupleKey(rid, "editor", uid),
				tuple.NewTupleKey(rid, "admin", uid),
				tuple.NewTupleKey(rid, "banned", "user:evil"),
				tuple.NewTupleKey(gid, "member", uid),
				tuple.NewTupleKey(tid, "member", uid),
				tuple.NewTupleKey(rid, "viewer", gid+"#member"),
				tuple.NewTupleKey(rid, "viewer", tid+"#member"),
			)

			// Parent and related links
			if d > 0 {
				parentRid := fmt.Sprintf("resource:l%ds%d", d-1, b)
				tuples = append(tuples, tuple.NewTupleKey(rid, "parent", parentRid))
				tuples = append(tuples, tuple.NewTupleKey(rid, "related", parentRid))
			}
			if b > 0 {
				relatedRid := fmt.Sprintf("resource:l%ds%d", d, b-1)
				tuples = append(tuples, tuple.NewTupleKey(rid, "related", relatedRid))
			}
		}
	}

	// Add a wildcard group and team at the deepest level
	tuples = append(tuples,
		tuple.NewTupleKey("group:gwild", "member", "user:*"),
		tuple.NewTupleKey("team:twild", "member", "user:*"),
		tuple.NewTupleKey("resource:l6s0", "viewer", "group:gwild#member"),
		tuple.NewTupleKey("resource:l6s0", "viewer", "team:twild#member"),
	)

	ctx, checker := newLocalChecker(t, model, tuples...)

	// Test a user at the deepest level, breadth 0, via wildcard group and team
	checkAllowed(t, ctx, checker, "resource:l6s0", "viewer", "user:someone", true)
	checkAllowed(t, ctx, checker, "resource:l6s0", "viewer", "user:evil", false) // banned everywhere

	// Test a user who is editor and admin at every level and breadth
	for d := 0; d <= depth; d++ {
		for b := 0; b < breadth; b++ {
			uid := fmt.Sprintf("user:d%db%d", d, b)
			rid := fmt.Sprintf("resource:l%ds%d", d, b)
			checkAllowed(t, ctx, checker, rid, "allowed", uid, true)
			checkAllowed(t, ctx, checker, rid, "viewer", uid, true)
		}
	}
}

func TestLocalChecker_ComplexMultiTypeTTUAndMultiLevelExclusion(t *testing.T) {
	model := `
model
  schema 1.1
type doc
  relations
    define parent: [doc]
    define related: [doc]
    define banned: [user] or banned from parent or banned from related
    define editor: [user] or editor from parent or editor from related
    define admin: [user] or admin from parent or admin from related
    define privileged: editor and admin
    define allowed: privileged but not banned
    define viewer: [user, group#member, team#member] or allowed or viewer from parent or viewer from related
type group
  relations
    define member: [user, user:*]
type team
  relations
    define member: [user, group#member]
type user
`
	// Build a graph with multiple TTUs, exclusions, and cross-links
	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("doc:root", "viewer", "user:alice"),
		tuple.NewTupleKey("doc:root", "banned", "user:bob"),
		tuple.NewTupleKey("group:everyone", "member", "user:*"),
		tuple.NewTupleKey("team:all", "member", "user:*"),
		tuple.NewTupleKey("doc:root", "viewer", "group:everyone#member"),
		tuple.NewTupleKey("doc:root", "viewer", "team:all#member"),
	}
	// Create a chain and cross-links
	for i := 1; i <= 8; i++ {
		child := fmt.Sprintf("doc:n%02d", i)
		parent := "doc:root"
		if i > 1 {
			parent = fmt.Sprintf("doc:n%02d", i-1)
		}
		tuples = append(tuples,
			tuple.NewTupleKey(child, "parent", parent),
			tuple.NewTupleKey(child, "related", "doc:root"),
			tuple.NewTupleKey(child, "banned", "user:bob"),
			tuple.NewTupleKey(child, "editor", "user:alice"),
			tuple.NewTupleKey(child, "admin", "user:alice"),
		)
	}

	ctx, checker := newLocalChecker(t, model, tuples...)

	// alice should be allowed at the deepest node
	checkAllowed(t, ctx, checker, "doc:n08", "allowed", "user:alice", true)
	// bob should not be allowed at any node due to multi-level exclusion
	checkAllowed(t, ctx, checker, "doc:n08", "allowed", "user:bob", false)
	// someone should be allowed via wildcard group/team
	checkAllowed(t, ctx, checker, "doc:n08", "viewer", "user:someone", true)
}
