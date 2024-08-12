package main

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

const (
	numItems    = 100_000_000
	benchLookup = 1_000_000
)

func uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}

func generateRandomNumbers(n int) [][]byte {
	numbers := make([][]byte, n)
	for i := range numbers {
		numbers[i] = uint64ToBytes(rand.Uint64())
	}
	return numbers
}

func TestQuotientFilterBasic(t *testing.T) {
	qf := NewQuotientFilter(10)

	testItems := []uint64{1, 100, 1000, 10000, 100000}
	for _, item := range testItems {
		qf.Insert(uint64ToBytes(item))
		exists, _ := qf.Exists(uint64ToBytes(item))
		if !exists {
			t.Errorf("Item %d should exist in the filter, but doesn't", item)
		}
	}

	nonExistentItems := []uint64{2, 200, 2000, 20000, 200000}
	for _, item := range nonExistentItems {
		exists, _ := qf.Exists(uint64ToBytes(item))
		if exists {
			t.Errorf("Item %d should not exist in the filter, but does", item)
		}
	}
}

func TestQuotientFilterDuplicates(t *testing.T) {
	qf := NewQuotientFilter(8)
	testData := []byte("test")

	t.Log("Inserting first item")
	err := qf.Insert(testData)
	if err != nil {
		t.Fatalf("Failed to insert item: %v", err)
	}

	err = qf.Insert(testData)
	if err != nil {
		t.Fatalf("Failed to insert duplicate item: %v", err)
	}

	t.Log("Checking if item exists")
	exists, duration := qf.Exists(testData)
	t.Logf("Exists check took %v", duration)

	if !exists {
		t.Error("Item should exist in the filter")
	}

	count := qf.Count()
	if count != 1 {
		t.Errorf("Expected 1 item in the filter, but found %d", count)
	}

	// Test inserting a different item
	differentData := []byte("different")
	err = qf.Insert(differentData)
	if err != nil {
		t.Fatalf("Failed to insert different item: %v", err)
	}

	count = qf.Count()
	if count != 2 {
		t.Errorf("Expected 2 items in the filter after inserting a different item, but found %d", count)
	}
}

func TestQuotientFilterCapacity(t *testing.T) {
	const logSize = 8 // 2^8 = 256 slots
	qf := NewQuotientFilter(logSize)
	capacity := 1 << logSize

	rand.Seed(time.Now().UnixNano())
	numbers := make(map[uint64]bool)
	for len(numbers) < capacity {
		numbers[rand.Uint64()] = true
	}

	insertionFailures := 0
	for num := range numbers {
		err := qf.Insert(uint64ToBytes(num))
		if err != nil {
			insertionFailures++
		}
	}

	count := qf.Count()
	if int(count) != len(numbers)-insertionFailures {
		t.Errorf("Expected %d items, but filter reports %d", len(numbers)-insertionFailures, count)
	}
}

func TestQuotientFilterFalseNegatives(t *testing.T) {
	const logSize = 22 // 2^22 = 4,194,304 slots
	qf := NewQuotientFilter(logSize)
	capacity := 1 << logSize

	// Calculate the number of items to insert (50% of capacity)
	numItems := int(float64(capacity) * 0.5)

	rand.Seed(time.Now().UnixNano())
	numbers := make(map[uint64]bool)
	for len(numbers) < numItems {
		numbers[rand.Uint64()] = true
	}

	for num := range numbers {
		err := qf.Insert(uint64ToBytes(num))
		if err != nil {
			t.Fatalf("Failed to insert item: %v", err)
		}
	}

	falseNegatives := 0
	for num := range numbers {
		exists, _ := qf.Exists(uint64ToBytes(num))
		if !exists {
			falseNegatives++
		}
	}

	falseNegativeRate := float64(falseNegatives) / float64(len(numbers))
	t.Logf("Items inserted: %d", len(numbers))
	t.Logf("False negatives: %d (%.6f%%)", falseNegatives, falseNegativeRate*100)

	acceptableRate := 0.2 // @todo: decrease that to 0.01 (1%)
	if falseNegativeRate > acceptableRate {
		t.Errorf("False negative rate too high: %.6f%% (threshold: %.6f%%)", falseNegativeRate*100, acceptableRate*100)
	}
}

func TestQuotientFilterFalsePositives(t *testing.T) {
	const logSize = 8 // 2^8 = 256 slots
	qf := NewQuotientFilter(logSize)
	capacity := 1 << logSize

	// Generate random numbers for insertion
	rand.Seed(time.Now().UnixNano())
	numbers := make(map[uint64]bool)
	for len(numbers) < capacity {
		numbers[rand.Uint64()] = true
	}

	for num := range numbers {
		_ = qf.Insert(uint64ToBytes(num))
	}

	falsePositives := 0
	testsCount := 10000
	for i := 0; i < testsCount; i++ {
		num := rand.Uint64()
		if !numbers[num] {
			exists, _ := qf.Exists(uint64ToBytes(num))
			if exists {
				falsePositives++
			}
		}
	}
	falsePositiveRate := float64(falsePositives) / float64(testsCount)
	t.Logf("False positive rate: %.4f", falsePositiveRate)
	// Optionally assert that the false positive rate is within expected bounds
	if falsePositiveRate > 0.01 { // Example threshold, adjust as needed
		t.Errorf("False positive rate too high: %.4f", falsePositiveRate)
	}
}

func TestQuotientFilterOverflow(t *testing.T) {
	const logSize = 14 // 2^14 = 16,384 slots
	qf := NewQuotientFilter(logSize)
	capacity := 1 << logSize

	// Generate random numbers for insertion
	rand.Seed(time.Now().UnixNano())
	numbers := make(map[uint64]bool)
	for len(numbers) < capacity {
		numbers[rand.Uint64()] = true
	}

	for num := range numbers {
		_ = qf.Insert(uint64ToBytes(num))
	}

	extraInsertions := capacity / 4 // Try inserting 25% more items
	extraInsertionFailures := 0
	for i := 0; i < extraInsertions; i++ {
		num := rand.Uint64()
		err := qf.Insert(uint64ToBytes(num))
		if err != nil {
			extraInsertionFailures++
		} else {
			numbers[num] = true
		}
	}

	finalCount := qf.Count()
	t.Logf("Final count after extra insertions: %d", finalCount)

	falseNegatives := 0
	for num := range numbers {
		exists, _ := qf.Exists(uint64ToBytes(num))
		if !exists {
			falseNegatives++
		}
	}
	falseNegativeRate := float64(falseNegatives) / float64(len(numbers))
	t.Logf("Final false negative rate: %.4f", falseNegativeRate)

	if falseNegativeRate > 0.05 { // Allow up to 5% false negative rate. @todo: lower to 1%
		t.Errorf("False negative rate too high: %.4f", falseNegativeRate)
	}
}

func TestQuotientFilterEdgeCases(t *testing.T) {
	qf := NewQuotientFilter(10)

	qf.Insert(uint64ToBytes(0))
	exists, _ := qf.Exists(uint64ToBytes(0))
	if !exists {
		t.Error("0 should exist in the filter, but doesn't")
	}

	maxUint64 := uint64(^uint64(0))
	qf.Insert(uint64ToBytes(maxUint64))
	exists, _ = qf.Exists(uint64ToBytes(maxUint64))
	if !exists {
		t.Error("Maximum uint64 value should exist in the filter, but doesn't")
	}
}

func TestQuotientFilterRemove(t *testing.T) {
	t.Run("Remove existing item", func(t *testing.T) {
		qf := NewQuotientFilter(8)
		item := uint64ToBytes(42)

		err := qf.Insert(item)
		if err != nil {
			t.Fatalf("Failed to insert item: %v", err)
		}

		removed := qf.Remove(item)
		if !removed {
			t.Error("Failed to remove existing item")
		}

		exists, _ := qf.Exists(item)
		if exists {
			t.Error("Item still exists after removal")
		}

		if qf.Count() != 0 {
			t.Errorf("Expected count 0, got %d", qf.Count())
		}
	})

	t.Run("Remove non-existing item", func(t *testing.T) {
		qf := NewQuotientFilter(8)
		item := uint64ToBytes(42)

		removed := qf.Remove(item)
		if removed {
			t.Error("Reported removal of non-existing item")
		}

		if qf.Count() != 0 {
			t.Errorf("Expected count 0, got %d", qf.Count())
		}
	})

	t.Run("Remove from multiple items", func(t *testing.T) {
		qf := NewQuotientFilter(8)
		items := []uint64{1, 2, 3, 4, 5}

		for _, item := range items {
			err := qf.Insert(uint64ToBytes(item))
			if err != nil {
				t.Fatalf("Failed to insert item %d: %v", item, err)
			}
		}

		removedItem := uint64ToBytes(3)
		removed := qf.Remove(removedItem)
		if !removed {
			t.Error("Failed to remove existing item")
		}

		if qf.Count() != 4 {
			t.Errorf("Expected count 4, got %d", qf.Count())
		}

		for _, item := range items {
			exists, _ := qf.Exists(uint64ToBytes(item))
			if item == 3 && exists {
				t.Error("Removed item still exists")
			} else if item != 3 && !exists {
				t.Errorf("Item %d should exist but doesn't", item)
			}
		}
	})

	t.Run("Remove and reinsert", func(t *testing.T) {
		qf := NewQuotientFilter(8)
		item := uint64ToBytes(42)

		err := qf.Insert(item)
		if err != nil {
			t.Fatalf("Failed to insert item: %v", err)
		}

		removed := qf.Remove(item)
		if !removed {
			t.Error("Failed to remove existing item")
		}

		err = qf.Insert(item)
		if err != nil {
			t.Fatalf("Failed to reinsert item: %v", err)
		}

		exists, _ := qf.Exists(item)
		if !exists {
			t.Error("Reinserted item doesn't exist")
		}

		if qf.Count() != 1 {
			t.Errorf("Expected count 1, got %d", qf.Count())
		}
	})

	t.Run("Remove with collisions", func(t *testing.T) {
		qf := NewQuotientFilter(4) // Small filter to force collisions
		items := generateRandomNumbers(10)

		for _, item := range items {
			err := qf.Insert(item)
			if err != nil {
				t.Fatalf("Failed to insert item: %v", err)
			}
		}

		initialCount := qf.Count()
		removedItem := items[5]
		removed := qf.Remove(removedItem)
		if !removed {
			t.Error("Failed to remove existing item")
		}

		if qf.Count() != initialCount-1 {
			t.Errorf("Expected count %d, got %d", initialCount-1, qf.Count())
		}

		exists, _ := qf.Exists(removedItem)
		if exists {
			t.Error("Removed item still exists")
		}

		for i, item := range items {
			if i != 5 {
				exists, _ := qf.Exists(item)
				if !exists {
					t.Errorf("Item at index %d should exist but doesn't", i)
				}
			}
		}
	})
}

//func TestQuotientFilterRemoveWithMap(t *testing.T) {
//	t.Run("Remove with collisions", func(t *testing.T) {
//		qf := NewQuotientFilter(4)
//
//		items := []string{
//			"item1", "item2", "item3", "item4",
//			"item5", "item6", "item7", "item8",
//			"item9", "item10",
//		}
//
//		fmt.Println("Inserting items:")
//		for _, item := range items {
//			err := qf.Insert([]byte(item))
//			if err != nil {
//				t.Fatalf("Failed to insert item %s: %v", item, err)
//			}
//		}
//
//		fmt.Println("Checking existence of items:")
//		for i, item := range items {
//			exists, _ := qf.Exists([]byte(item))
//			if !exists {
//				t.Errorf("Item at index %d (%s) should exist but doesn't", i, item)
//			}
//		}
//
//		fmt.Println("\nRemoving even-indexed items:")
//		for i := 0; i < len(items); i += 2 {
//			removed := qf.Remove([]byte(items[i]))
//			if !removed {
//				t.Errorf("Failed to remove item at index %d (%s)", i, items[i])
//			}
//		}
//
//		fmt.Println("Checking final existence of items:")
//		for i, item := range items {
//			exists, _ := qf.Exists([]byte(item))
//			if i%2 == 0 {
//				if exists {
//					t.Errorf("Item at index %d (%s) should not exist but does", i, item)
//				}
//			} else {
//				if !exists {
//					t.Errorf("Item at index %d (%s) should exist but doesn't", i, item)
//				}
//			}
//		}
//
//		expectedCount := len(items) / 2
//		if qf.Count() != expectedCount {
//			t.Errorf("Expected count of %d, but got %d", expectedCount, qf.Count())
//		}
//	})
//}
