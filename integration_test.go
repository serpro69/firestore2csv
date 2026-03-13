//go:build integration

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/latlng"
)

const testProject = "test-project"

func TestMain(m *testing.M) {
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		fmt.Println("skipping integration tests: FIRESTORE_EMULATOR_HOST not set")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func newTestClient(t *testing.T) *firestore.Client {
	t.Helper()
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, testProject)
	if err != nil {
		t.Fatalf("failed to create Firestore client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// seedFirestore populates the emulator with test data and returns cleanup function.
func seedFirestore(t *testing.T, client *firestore.Client) {
	t.Helper()
	ctx := context.Background()

	now := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	// Users collection
	users := []struct {
		id   string
		data map[string]any
	}{
		{"user1", map[string]any{"name": "Alice", "age": int64(30), "active": true, "created": now}},
		{"user2", map[string]any{"name": "Bob", "age": int64(25), "active": false, "created": now.Add(time.Hour)}},
		{"user3", map[string]any{"name": "Charlie", "age": int64(35), "active": true, "created": now.Add(2 * time.Hour)}},
	}
	for _, u := range users {
		if _, err := client.Collection("users").Doc(u.id).Set(ctx, u.data); err != nil {
			t.Fatalf("failed to seed user %s: %v", u.id, err)
		}
	}

	// Products collection
	products := []struct {
		id   string
		data map[string]any
	}{
		{"prod1", map[string]any{
			"title":    "Widget",
			"price":    float64(9.99),
			"tags":     []any{"sale", "new"},
			"location": &latlng.LatLng{Latitude: 37.7749, Longitude: -122.4194},
		}},
		{"prod2", map[string]any{
			"title":    "Gadget",
			"price":    float64(19.99),
			"tags":     []any{"premium"},
			"location": &latlng.LatLng{Latitude: 40.7128, Longitude: -74.0060},
		}},
	}
	for _, p := range products {
		if _, err := client.Collection("products").Doc(p.id).Set(ctx, p.data); err != nil {
			t.Fatalf("failed to seed product %s: %v", p.id, err)
		}
	}

	// Sub-collections: users/{id}/orders
	orders := map[string][]struct {
		id   string
		data map[string]any
	}{
		"user1": {
			{"order1", map[string]any{"amount": float64(100.50), "date": now}},
			{"order2", map[string]any{"amount": float64(200.75), "date": now.Add(24 * time.Hour)}},
		},
		"user2": {
			{"order3", map[string]any{"amount": float64(50.00), "date": now}},
			{"order4", map[string]any{"amount": float64(75.25), "date": now.Add(24 * time.Hour)}},
		},
	}
	for userID, userOrders := range orders {
		for _, o := range userOrders {
			if _, err := client.Collection("users").Doc(userID).Collection("orders").Doc(o.id).Set(ctx, o.data); err != nil {
				t.Fatalf("failed to seed order %s for %s: %v", o.id, userID, err)
			}
		}
	}

	// Sub-sub-collections: users/{id}/orders/{id}/items
	items := map[string]map[string]struct {
		id   string
		data map[string]any
	}{
		"user1": {
			"order1": {"item1", map[string]any{"sku": "SKU-001", "qty": int64(2)}},
			"order2": {"item2", map[string]any{"sku": "SKU-002", "qty": int64(1)}},
		},
		"user2": {
			"order3": {"item3", map[string]any{"sku": "SKU-003", "qty": int64(3)}},
		},
	}
	for userID, userItems := range items {
		for orderID, item := range userItems {
			ref := client.Collection("users").Doc(userID).Collection("orders").Doc(orderID).Collection("items").Doc(item.id)
			if _, err := ref.Set(ctx, item.data); err != nil {
				t.Fatalf("failed to seed item %s: %v", item.id, err)
			}
		}
	}

	t.Cleanup(func() {
		cleanFirestore(t, client)
	})
}

// cleanFirestore deletes all documents from known test collections.
func cleanFirestore(t *testing.T, client *firestore.Client) {
	t.Helper()
	ctx := context.Background()

	for _, name := range []string{"users", "products", "imported_users", "imported_products", "target", "heuristic_target", "virtual_parents"} {
		deleteCollection(ctx, t, client, client.Collection(name))
	}
}

func deleteCollection(ctx context.Context, t *testing.T, client *firestore.Client, col *firestore.CollectionRef) {
	t.Helper()
	docs, err := col.Documents(ctx).GetAll()
	if err != nil {
		return
	}
	for _, doc := range docs {
		// Delete sub-collections first
		subIter := doc.Ref.Collections(ctx)
		for {
			subCol, err := subIter.Next()
			if err != nil {
				break
			}
			deleteCollection(ctx, t, client, subCol)
		}
		if _, err := doc.Ref.Delete(ctx); err != nil {
			t.Logf("warning: failed to delete %s: %v", doc.Ref.Path, err)
		}
	}
}

// readTestCSV reads all records from a CSV file.
func readTestCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %s: %v", path, err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("failed to read CSV %s: %v", path, err)
	}
	return records
}

func TestResolveCollections_All(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	ctx := context.Background()
	names, err := resolveCollections(ctx, client, "")
	if err != nil {
		t.Fatalf("resolveCollections() error = %v", err)
	}

	// Should find at least "products" and "users" (sorted)
	if len(names) < 2 {
		t.Fatalf("expected at least 2 collections, got %d: %v", len(names), names)
	}

	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["users"] {
		t.Error("expected 'users' in collections")
	}
	if !found["products"] {
		t.Error("expected 'products' in collections")
	}
}

func TestResolveCollections_Filtered(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	ctx := context.Background()
	names, err := resolveCollections(ctx, client, "users")
	if err != nil {
		t.Fatalf("resolveCollections() error = %v", err)
	}

	if len(names) != 1 || names[0] != "users" {
		t.Errorf("expected [users], got %v", names)
	}
}

func TestExportSingleCollection(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	tmpDir := t.TempDir()
	ctx := context.Background()

	results := exportCollectionTree(ctx, client, "users", 0, 0, 0, tmpDir, false, nil)

	// depth=0 means top-level only
	if len(results) != 1 {
		t.Fatalf("expected 1 result (top-level only), got %d", len(results))
	}

	r := results[0]
	if r.err != nil {
		t.Fatalf("export error: %v", r.err)
	}
	if r.docCount != 3 {
		t.Errorf("docCount = %d, want 3", r.docCount)
	}

	csvPath := filepath.Join(tmpDir, "users.csv")
	records := readTestCSV(t, csvPath)
	if len(records) != 4 { // header + 3 docs
		t.Errorf("expected 4 rows, got %d", len(records))
	}
}

func TestExportWithSubCollections(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	tmpDir := t.TempDir()
	ctx := context.Background()

	// depth=-1 means unlimited recursion
	results := exportCollectionTree(ctx, client, "users", 0, 0, -1, tmpDir, false, nil)

	// Should have users + users/orders + users/orders/items
	if len(results) < 3 {
		t.Fatalf("expected at least 3 results, got %d", len(results))
	}

	// Verify files exist
	for _, path := range []string{
		filepath.Join(tmpDir, "users.csv"),
		filepath.Join(tmpDir, "users", "orders.csv"),
		filepath.Join(tmpDir, "users", "orders", "items.csv"),
	} {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s to exist", path)
		}
	}

	// Verify orders CSV has 4 rows (2 per user1 + 2 per user2)
	ordersRecords := readTestCSV(t, filepath.Join(tmpDir, "users", "orders.csv"))
	if len(ordersRecords) != 5 { // header + 4 orders
		t.Errorf("orders: expected 5 rows, got %d", len(ordersRecords))
	}
}

func TestExportDepthLimit(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	tmpDir := t.TempDir()
	ctx := context.Background()

	// depth=1 means users + orders but NOT items
	results := exportCollectionTree(ctx, client, "users", 0, 0, 1, tmpDir, false, nil)

	// Should have users + users/orders only
	collections := map[string]bool{}
	for _, r := range results {
		collections[r.collection] = true
	}

	if !collections["users"] {
		t.Error("expected 'users' in results")
	}
	if !collections["users/orders"] {
		t.Error("expected 'users/orders' in results")
	}
	if collections["users/orders/items"] {
		t.Error("did not expect 'users/orders/items' with depth=1")
	}

	// items.csv should NOT exist
	itemsPath := filepath.Join(tmpDir, "users", "orders", "items.csv")
	if _, err := os.Stat(itemsPath); !os.IsNotExist(err) {
		t.Error("items.csv should not exist with depth=1")
	}
}

func TestExportWithLimit(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	tmpDir := t.TempDir()
	ctx := context.Background()

	results := exportCollectionTree(ctx, client, "users", 1, 0, 0, tmpDir, false, nil)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].docCount != 1 {
		t.Errorf("docCount = %d, want 1", results[0].docCount)
	}

	records := readTestCSV(t, filepath.Join(tmpDir, "users.csv"))
	if len(records) != 2 { // header + 1 doc
		t.Errorf("expected 2 rows, got %d", len(records))
	}
}

func TestFormatValue_DocumentRef(t *testing.T) {
	client := newTestClient(t)
	ref := client.Doc("users/user1")

	got := formatValue(ref)
	want := "projects/test-project/databases/(default)/documents/users/user1"
	if got != want {
		t.Errorf("formatValue(DocumentRef) = %q, want %q", got, want)
	}
}

func TestRunExport_FullPipeline(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	tmpDir := t.TempDir()
	err := runExport(exportConfig{
		project:     testProject,
		database:    "(default)",
		collections: "users,products",
		limit:       0,
		childLimit:  0,
		maxDepth:    -1,
		output:      tmpDir,
	})
	if err != nil {
		t.Fatalf("runExport() error = %v", err)
	}

	// Verify users.csv
	usersRecords := readTestCSV(t, filepath.Join(tmpDir, "users.csv"))
	if len(usersRecords) != 4 { // header + 3 users
		t.Errorf("users: expected 4 rows, got %d", len(usersRecords))
	}

	// Verify products.csv
	productsRecords := readTestCSV(t, filepath.Join(tmpDir, "products.csv"))
	if len(productsRecords) != 3 { // header + 2 products
		t.Errorf("products: expected 3 rows, got %d", len(productsRecords))
	}

	// Verify sub-collection files exist
	for _, path := range []string{
		filepath.Join(tmpDir, "users", "orders.csv"),
		filepath.Join(tmpDir, "users", "orders", "items.csv"),
	} {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s to exist", path)
		}
	}
}

// --- Import integration tests ---

// rewriteCSVPaths reads a CSV file and rewrites the __path__ column to use
// a different collection prefix. Returns the path to the rewritten CSV.
func rewriteCSVPaths(t *testing.T, srcPath, oldPrefix, newPrefix, destDir string) string {
	t.Helper()
	records := readTestCSV(t, srcPath)
	if len(records) < 1 {
		t.Fatalf("CSV %s has no rows", srcPath)
	}

	// Find __path__ column
	pathIdx := -1
	for i, h := range records[0] {
		if h == "__path__" {
			pathIdx = i
			break
		}
	}
	if pathIdx < 0 {
		t.Fatalf("CSV %s missing __path__ column", srcPath)
	}

	// Rewrite paths
	for i := 1; i < len(records); i++ {
		records[i][pathIdx] = strings.Replace(records[i][pathIdx], oldPrefix, newPrefix, 1)
	}

	destPath := filepath.Join(destDir, filepath.Base(srcPath))
	f, err := os.Create(destPath)
	if err != nil {
		t.Fatalf("creating %s: %v", destPath, err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.WriteAll(records)
	w.Flush()
	return destPath
}

// readAllDocs reads all documents from a collection path in Firestore.
func readAllDocs(t *testing.T, client *firestore.Client, collectionPath string) map[string]map[string]any {
	t.Helper()
	ctx := context.Background()
	docs, err := client.Collection(collectionPath).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("reading collection %s: %v", collectionPath, err)
	}
	result := make(map[string]map[string]any, len(docs))
	for _, doc := range docs {
		result[doc.Ref.ID] = doc.Data()
	}
	return result
}

func TestImportRoundTrip(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	// Step 1: Export users with --with-types
	exportDir := t.TempDir()
	err := runExport(exportConfig{
		project:     testProject,
		database:    "(default)",
		collections: "users",
		maxDepth:    0,
		output:      exportDir,
		withTypes:   true,
	})
	if err != nil {
		t.Fatalf("export error: %v", err)
	}

	// Step 2: Rewrite paths from users/* to imported_users/*
	importDir := t.TempDir()
	rewriteCSVPaths(t, filepath.Join(exportDir, "users.csv"), "users/", "imported_users/", importDir)

	// Step 3: Import into imported_users collection
	err = runImport(importConfig{
		project:    testProject,
		database:   "(default)",
		inputs:     []string{importDir},
		onConflict: "overwrite",
	})
	if err != nil {
		t.Fatalf("import error: %v", err)
	}

	// Step 4: Read back and compare
	origDocs := readAllDocs(t, client, "users")
	importedDocs := readAllDocs(t, client, "imported_users")

	if len(importedDocs) != len(origDocs) {
		t.Fatalf("imported %d docs, want %d", len(importedDocs), len(origDocs))
	}

	for id, orig := range origDocs {
		imported, ok := importedDocs[id]
		if !ok {
			t.Errorf("missing imported doc %s", id)
			continue
		}
		// Compare field values
		for k, origVal := range orig {
			importedVal, ok := imported[k]
			if !ok {
				t.Errorf("doc %s: missing field %s", id, k)
				continue
			}
			// Compare by formatted value (handles type differences like time.Time)
			if formatValue(origVal) != formatValue(importedVal) {
				t.Errorf("doc %s field %s: got %v (%T), want %v (%T)",
					id, k, importedVal, importedVal, origVal, origVal)
			}
		}
	}
}

func TestImportConflictSkip(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)
	ctx := context.Background()

	// Pre-create a document in the target collection
	targetDoc := map[string]any{"name": "Original", "age": int64(99)}
	client.Collection("target").Doc("doc1").Set(ctx, targetDoc)

	// Create CSV with a conflicting doc and a new doc
	importDir := t.TempDir()
	csvContent := "__path__,name,age\ntarget/doc1,Replacement,50\ntarget/doc2,NewDoc,25\n"
	os.WriteFile(filepath.Join(importDir, "data.csv"), []byte(csvContent), 0644)

	err := runImport(importConfig{
		project:    testProject,
		database:   "(default)",

		inputs:     []string{importDir},
		onConflict: "skip",
	})
	if err != nil {
		t.Fatalf("import error: %v", err)
	}

	// doc1 should still have original data (skipped)
	doc1, err := client.Collection("target").Doc("doc1").Get(ctx)
	if err != nil {
		t.Fatalf("reading doc1: %v", err)
	}
	if doc1.Data()["name"] != "Original" {
		t.Errorf("doc1.name = %v, want Original (should have been skipped)", doc1.Data()["name"])
	}

	// doc2 should exist (new)
	doc2, err := client.Collection("target").Doc("doc2").Get(ctx)
	if err != nil {
		t.Fatalf("reading doc2: %v", err)
	}
	if doc2.Data()["name"] != "NewDoc" {
		t.Errorf("doc2.name = %v, want NewDoc", doc2.Data()["name"])
	}
}

func TestImportConflictOverwrite(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)
	ctx := context.Background()

	// Pre-create a document
	client.Collection("target").Doc("doc1").Set(ctx, map[string]any{"name": "Original", "extra": "field"})

	importDir := t.TempDir()
	csvContent := "__path__,name,age\ntarget/doc1,Replaced,42\n"
	os.WriteFile(filepath.Join(importDir, "data.csv"), []byte(csvContent), 0644)

	err := runImport(importConfig{
		project:    testProject,
		database:   "(default)",

		inputs:     []string{importDir},
		onConflict: "overwrite",
	})
	if err != nil {
		t.Fatalf("import error: %v", err)
	}

	doc1, err := client.Collection("target").Doc("doc1").Get(ctx)
	if err != nil {
		t.Fatalf("reading doc1: %v", err)
	}
	data := doc1.Data()
	if data["name"] != "Replaced" {
		t.Errorf("doc1.name = %v, want Replaced", data["name"])
	}
	// "extra" field should be gone (overwrite replaces entire doc)
	if _, ok := data["extra"]; ok {
		t.Error("doc1 should not have 'extra' field after overwrite")
	}
}

func TestImportConflictMerge(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)
	ctx := context.Background()

	// Pre-create a document with an extra field
	client.Collection("target").Doc("doc1").Set(ctx, map[string]any{"name": "Original", "extra": "kept"})

	importDir := t.TempDir()
	csvContent := "__path__,name,age\ntarget/doc1,Merged,42\n"
	os.WriteFile(filepath.Join(importDir, "data.csv"), []byte(csvContent), 0644)

	err := runImport(importConfig{
		project:    testProject,
		database:   "(default)",

		inputs:     []string{importDir},
		onConflict: "merge",
	})
	if err != nil {
		t.Fatalf("import error: %v", err)
	}

	doc1, err := client.Collection("target").Doc("doc1").Get(ctx)
	if err != nil {
		t.Fatalf("reading doc1: %v", err)
	}
	data := doc1.Data()
	if data["name"] != "Merged" {
		t.Errorf("doc1.name = %v, want Merged", data["name"])
	}
	// "extra" field should be preserved (merge keeps existing fields)
	if data["extra"] != "kept" {
		t.Errorf("doc1.extra = %v, want kept", data["extra"])
	}
}

func TestImportConflictFail(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)
	ctx := context.Background()

	// Pre-create a conflicting document
	client.Collection("target").Doc("doc1").Set(ctx, map[string]any{"name": "Existing"})

	importDir := t.TempDir()
	csvContent := "__path__,name\ntarget/doc1,New\ntarget/doc2,AlsoNew\n"
	os.WriteFile(filepath.Join(importDir, "data.csv"), []byte(csvContent), 0644)

	err := runImport(importConfig{
		project:    testProject,
		database:   "(default)",

		inputs:     []string{importDir},
		onConflict: "fail",
	})
	if err == nil {
		t.Fatal("expected error for conflict with --on-conflict=fail")
	}
	if !strings.Contains(err.Error(), "conflicting") {
		t.Errorf("unexpected error: %v", err)
	}

	// doc2 should NOT have been written (abort before writing)
	_, err = client.Collection("target").Doc("doc2").Get(ctx)
	if err == nil {
		t.Error("doc2 should not exist — import should have aborted before writing")
	}
}

func TestImportSubCollectionRoundTrip(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	// Export users with sub-collections and types
	exportDir := t.TempDir()
	err := runExport(exportConfig{
		project:     testProject,
		database:    "(default)",
		collections: "users",
		maxDepth:    -1,
		output:      exportDir,
		withTypes:   true,
	})
	if err != nil {
		t.Fatalf("export error: %v", err)
	}

	// Rewrite all exported CSVs to use imported_users prefix
	importDir := t.TempDir()
	rewriteCSVPaths(t, filepath.Join(exportDir, "users.csv"), "users/", "imported_users/", importDir)

	ordersDir := filepath.Join(importDir, "users")
	os.MkdirAll(ordersDir, 0755)
	rewriteCSVPaths(t, filepath.Join(exportDir, "users", "orders.csv"), "users/", "imported_users/", ordersDir)

	itemsDir := filepath.Join(importDir, "users", "orders")
	os.MkdirAll(itemsDir, 0755)
	rewriteCSVPaths(t, filepath.Join(exportDir, "users", "orders", "items.csv"), "users/", "imported_users/", itemsDir)

	// Import
	err = runImport(importConfig{
		project:    testProject,
		database:   "(default)",
		inputs:     []string{importDir},
		onConflict: "overwrite",
	})
	if err != nil {
		t.Fatalf("import error: %v", err)
	}

	// Verify top-level docs
	importedUsers := readAllDocs(t, client, "imported_users")
	if len(importedUsers) != 3 {
		t.Errorf("expected 3 imported users, got %d", len(importedUsers))
	}

	// Verify sub-collection: imported_users/user1/orders
	ctx := context.Background()
	orderDocs, err := client.Collection("imported_users").Doc("user1").Collection("orders").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("reading orders: %v", err)
	}
	if len(orderDocs) != 2 {
		t.Errorf("expected 2 orders for user1, got %d", len(orderDocs))
	}

	// Verify sub-sub-collection: imported_users/user1/orders/order1/items
	itemDocs, err := client.Collection("imported_users").Doc("user1").Collection("orders").Doc("order1").Collection("items").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("reading items: %v", err)
	}
	if len(itemDocs) != 1 {
		t.Errorf("expected 1 item for order1, got %d", len(itemDocs))
	}
}

func TestImportDryRun(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	importDir := t.TempDir()
	csvContent := "__path__,name\ntarget/dry1,A\ntarget/dry2,B\n"
	os.WriteFile(filepath.Join(importDir, "data.csv"), []byte(csvContent), 0644)

	err := runImport(importConfig{
		project:    testProject,
		database:   "(default)",

		inputs:     []string{importDir},
		onConflict: "skip",
		dryRun:     true,
	})
	if err != nil {
		t.Fatalf("dry-run error: %v", err)
	}

	// Verify nothing was written
	ctx := context.Background()
	docs, err := client.Collection("target").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("reading target collection: %v", err)
	}
	// Filter for dry1/dry2 specifically
	for _, doc := range docs {
		if doc.Ref.ID == "dry1" || doc.Ref.ID == "dry2" {
			t.Errorf("document %s should not exist after dry-run", doc.Ref.ID)
		}
	}
}

func TestImportHeuristic(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	// CSV without __fs_types__ — types inferred by heuristic
	importDir := t.TempDir()
	csvContent := `__path__,name,age,active,score,created
heuristic_target/doc1,Alice,30,true,9.5,2024-06-15T12:00:00Z
`
	os.WriteFile(filepath.Join(importDir, "data.csv"), []byte(csvContent), 0644)

	err := runImport(importConfig{
		project:    testProject,
		database:   "(default)",

		inputs:     []string{importDir},
		onConflict: "overwrite",
	})
	if err != nil {
		t.Fatalf("import error: %v", err)
	}

	ctx := context.Background()
	doc, err := client.Collection("heuristic_target").Doc("doc1").Get(ctx)
	if err != nil {
		t.Fatalf("reading doc: %v", err)
	}
	data := doc.Data()

	// Verify heuristic type detection
	if data["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", data["name"])
	}
	if data["age"] != int64(30) {
		t.Errorf("age = %v (%T), want int64(30)", data["age"], data["age"])
	}
	if data["active"] != true {
		t.Errorf("active = %v, want true", data["active"])
	}
	if data["score"] != float64(9.5) {
		t.Errorf("score = %v (%T), want float64(9.5)", data["score"], data["score"])
	}
	if ts, ok := data["created"].(time.Time); !ok {
		t.Errorf("created = %v (%T), want time.Time", data["created"], data["created"])
	} else if !ts.Equal(time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("created = %v, want 2024-06-15T12:00:00Z", ts)
	}
}

// collectSubCollectionNames returns sorted names of sub-collections under a document.
func collectSubCollectionNames(t *testing.T, ref *firestore.DocumentRef) []string {
	t.Helper()
	ctx := context.Background()
	var names []string
	iter := ref.Collections(ctx)
	for {
		col, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("listing sub-collections: %v", err)
		}
		names = append(names, col.ID)
	}
	sort.Strings(names)
	return names
}

func TestExportWithSanitize(t *testing.T) {
	client := newTestClient(t)
	seedFirestore(t, client)

	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	san := newSanitizer(sanitizeConfig{Fields: map[string]string{
		"name": "firstName",
	}}, 42)

	ctx := context.Background()

	// First export with sanitization
	results1 := exportCollectionTree(ctx, client, "users", 0, 0, 0, tmpDir1, false, san)
	if len(results1) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results1))
	}
	if results1[0].err != nil {
		t.Fatalf("export error: %v", results1[0].err)
	}

	records1 := readTestCSV(t, filepath.Join(tmpDir1, "users.csv"))
	if len(records1) != 4 { // header + 3 users
		t.Fatalf("expected 4 rows, got %d", len(records1))
	}

	// Find header indices
	headers := records1[0]
	nameIdx := -1
	ageIdx := -1
	for i, h := range headers {
		switch h {
		case "name":
			nameIdx = i
		case "age":
			ageIdx = i
		}
	}
	if nameIdx == -1 {
		t.Fatal("name column not found")
	}
	if ageIdx == -1 {
		t.Fatal("age column not found")
	}

	// Verify configured field (name) is replaced
	originalNames := map[string]bool{"Alice": true, "Bob": true, "Charlie": true}
	for _, row := range records1[1:] {
		if originalNames[row[nameIdx]] {
			t.Errorf("name should have been sanitized, got original: %q", row[nameIdx])
		}
	}

	// Verify non-configured field (age) is untouched
	ageValues := map[string]bool{"30": true, "25": true, "35": true}
	for _, row := range records1[1:] {
		if !ageValues[row[ageIdx]] {
			t.Errorf("age should be untouched, got %q", row[ageIdx])
		}
	}

	// Second export with same seed — verify determinism
	san2 := newSanitizer(sanitizeConfig{Fields: map[string]string{
		"name": "firstName",
	}}, 42)

	results2 := exportCollectionTree(ctx, client, "users", 0, 0, 0, tmpDir2, false, san2)
	if results2[0].err != nil {
		t.Fatalf("second export error: %v", results2[0].err)
	}

	records2 := readTestCSV(t, filepath.Join(tmpDir2, "users.csv"))

	// Same seed should produce identical CSV content
	if len(records1) != len(records2) {
		t.Fatalf("row count mismatch: %d vs %d", len(records1), len(records2))
	}
	for i := range records1 {
		for j := range records1[i] {
			if records1[i][j] != records2[i][j] {
				t.Errorf("row %d col %d: %q vs %q", i, j, records1[i][j], records2[i][j])
			}
		}
	}
}

func TestSanitizeSubcommand(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "sanitized")

	// Create CSV files mimicking export output with nested structure
	usersCSV := "__path__,name,age,active\nusers/user1,Alice,30,true\nusers/user2,Bob,25,false\n"
	if err := os.MkdirAll(filepath.Join(inputDir, "users"), 0755); err != nil {
		t.Fatal(err)
	}
	ordersCSV := "__path__,amount,date\nusers/user1/orders/order1,100.50,2024-06-15T12:00:00Z\n"

	if err := os.WriteFile(filepath.Join(inputDir, "users.csv"), []byte(usersCSV), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "users", "orders.csv"), []byte(ordersCSV), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := sanitizeConfig{Fields: map[string]string{"name": "firstName"}}
	if err := runSanitize(cfg, inputDir, outputDir, 42); err != nil {
		t.Fatalf("runSanitize error: %v", err)
	}

	// Verify output directory structure
	outUsersCSV := filepath.Join(outputDir, "users.csv")
	outOrdersCSV := filepath.Join(outputDir, "users", "orders.csv")

	if _, err := os.Stat(outUsersCSV); err != nil {
		t.Fatalf("expected users.csv in output: %v", err)
	}
	if _, err := os.Stat(outOrdersCSV); err != nil {
		t.Fatalf("expected users/orders.csv in output: %v", err)
	}

	// Verify users.csv content
	records := readTestCSV(t, outUsersCSV)
	if len(records) != 3 { // header + 2 rows
		t.Fatalf("expected 3 rows, got %d", len(records))
	}

	// Header preserved
	if records[0][0] != "__path__" || records[0][1] != "name" {
		t.Errorf("headers changed: %v", records[0])
	}

	// __path__ untouched
	if records[1][0] != "users/user1" {
		t.Errorf("__path__ should be untouched, got %q", records[1][0])
	}

	// name replaced
	if records[1][1] == "Alice" || records[2][1] == "Bob" {
		t.Error("name column should have been sanitized")
	}

	// age and active untouched
	if records[1][2] != "30" || records[1][3] != "true" {
		t.Errorf("non-configured fields should be untouched: age=%q active=%q", records[1][2], records[1][3])
	}

	// Verify orders.csv is unchanged (no matching columns)
	orderRecords := readTestCSV(t, outOrdersCSV)
	if orderRecords[1][0] != "users/user1/orders/order1" {
		t.Errorf("orders __path__ changed: %q", orderRecords[1][0])
	}
	if orderRecords[1][1] != "100.50" {
		t.Errorf("orders amount changed: %q", orderRecords[1][1])
	}
}

// TestExportVirtualDocuments verifies that sub-collections are discovered and
// exported even when the parent documents contain no data (virtual documents).
// In Firestore, a document can exist solely as a container for sub-collections
// without having any fields; such documents are not returned by collection queries.
func TestExportVirtualDocuments(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	// Create sub-collection documents under "virtual" parent documents that have
	// no data of their own. We write directly to the sub-collection path, which
	// creates the parent document as a virtual (data-less) container.
	subDocs := []struct {
		parentID string
		childID  string
		data     map[string]any
	}{
		{"vdoc1", "child1", map[string]any{"value": "a"}},
		{"vdoc1", "child2", map[string]any{"value": "b"}},
		{"vdoc2", "child3", map[string]any{"value": "c"}},
	}
	for _, d := range subDocs {
		ref := client.Collection("virtual_parents").Doc(d.parentID).Collection("children").Doc(d.childID)
		if _, err := ref.Set(ctx, d.data); err != nil {
			t.Fatalf("failed to seed %s: %v", ref.Path, err)
		}
	}
	t.Cleanup(func() {
		// Clean up sub-collection docs, then virtual parents
		for _, d := range subDocs {
			ref := client.Collection("virtual_parents").Doc(d.parentID).Collection("children").Doc(d.childID)
			ref.Delete(ctx)
		}
		for _, id := range []string{"vdoc1", "vdoc2"} {
			client.Collection("virtual_parents").Doc(id).Delete(ctx)
		}
	})

	// Verify that the top-level collection query returns 0 docs (virtual docs).
	docs, err := client.Collection("virtual_parents").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(docs) != 0 {
		t.Fatalf("expected 0 documents from query, got %d (test setup issue)", len(docs))
	}

	tmpDir := t.TempDir()
	results := exportCollectionTree(ctx, client, "virtual_parents", 0, 0, -1, tmpDir, false, nil)

	// We should get 2 results: virtual_parents (0 docs) + virtual_parents/children
	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d: %+v", len(results), results)
	}

	// The top-level collection should have 0 docs (no CSV written)
	if results[0].docCount != 0 {
		t.Errorf("virtual_parents docCount = %d, want 0", results[0].docCount)
	}

	// The children sub-collection should have all 3 docs
	var childrenResult *exportResult
	for i := range results {
		if results[i].collection == "virtual_parents/children" {
			childrenResult = &results[i]
			break
		}
	}
	if childrenResult == nil {
		t.Fatal("expected 'virtual_parents/children' in results")
	}
	if childrenResult.docCount != 3 {
		t.Errorf("children docCount = %d, want 3", childrenResult.docCount)
	}

	// Verify the CSV file exists and has correct content
	csvPath := filepath.Join(tmpDir, "virtual_parents", "children.csv")
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		t.Fatalf("expected %s to exist", csvPath)
	}
	records := readTestCSV(t, csvPath)
	if len(records) != 4 { // header + 3 data rows
		t.Errorf("expected 4 CSV rows (header + 3), got %d", len(records))
	}
}

// TestExportVirtualDocumentsNested verifies that virtual document traversal works
// across multiple nesting levels: grandparent (virtual) → parent (virtual) → children (with data).
func TestExportVirtualDocumentsNested(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	// Create a deeply nested sub-collection where both the top-level and
	// intermediate documents are virtual (no data).
	ref := client.Collection("virtual_parents").Doc("deep1").Collection("middle").Doc("mid1").Collection("leaves").Doc("leaf1")
	if _, err := ref.Set(ctx, map[string]any{"label": "deep"}); err != nil {
		t.Fatalf("failed to seed: %v", err)
	}
	t.Cleanup(func() {
		ref.Delete(ctx)
		client.Collection("virtual_parents").Doc("deep1").Collection("middle").Doc("mid1").Delete(ctx)
		client.Collection("virtual_parents").Doc("deep1").Delete(ctx)
	})

	tmpDir := t.TempDir()
	results := exportCollectionTree(ctx, client, "virtual_parents", 0, 0, -1, tmpDir, false, nil)

	// Collect exported collection names
	collections := map[string]bool{}
	for _, r := range results {
		collections[r.collection] = true
	}

	if !collections["virtual_parents/middle"] {
		t.Error("expected 'virtual_parents/middle' in results")
	}
	if !collections["virtual_parents/middle/leaves"] {
		t.Error("expected 'virtual_parents/middle/leaves' in results")
	}

	// Verify leaves CSV
	csvPath := filepath.Join(tmpDir, "virtual_parents", "middle", "leaves.csv")
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		t.Fatalf("expected %s to exist", csvPath)
	}
	records := readTestCSV(t, csvPath)
	if len(records) != 2 { // header + 1 data row
		t.Errorf("expected 2 CSV rows, got %d", len(records))
	}
}
