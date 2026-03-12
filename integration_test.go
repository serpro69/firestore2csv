//go:build integration

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
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

	deleteCollection(ctx, t, client, client.Collection("users"))
	deleteCollection(ctx, t, client, client.Collection("products"))
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

	results := exportCollectionTree(ctx, client, "users", 0, 0, 0, tmpDir, false)

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
	results := exportCollectionTree(ctx, client, "users", 0, 0, -1, tmpDir, false)

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
	results := exportCollectionTree(ctx, client, "users", 0, 0, 1, tmpDir, false)

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

	results := exportCollectionTree(ctx, client, "users", 1, 0, 0, tmpDir, false)

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
