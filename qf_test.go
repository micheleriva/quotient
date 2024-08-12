package main

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"
)

const (
	numItems    = 100_000_000
	logSize     = 28
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
	qf := New(10)

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
	qf := New(8)
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
	qf := New(logSize)
	capacity := 1 << logSize

	// Generate random numbers for insertion
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

//func TestQuotientFilterFalseNegatives(t *testing.T) {
//	const logSize = 8 // 2^8 = 256 slots
//	qf := New(logSize)
//	capacity := 1 << logSize
//
//	// Generate random numbers for insertion
//	rand.Seed(time.Now().UnixNano())
//	numbers := make(map[uint64]bool)
//	for len(numbers) < capacity {
//		numbers[rand.Uint64()] = true
//	}
//
//	for num := range numbers {
//		_ = qf.Insert(uint64ToBytes(num))
//	}
//
//	falseNegatives := 0
//	for num := range numbers {
//		exists, _ := qf.Exists(uint64ToBytes(num))
//		if !exists {
//			falseNegatives++
//		}
//	}
//	falseNegativeRate := float64(falseNegatives) / float64(len(numbers))
//	t.Logf("False negatives: %d (%.2f%%)", falseNegatives, falseNegativeRate*100)
//	if falseNegatives > 0 {
//		t.Errorf("Filter has %d false negatives (%.2f%%)", falseNegatives, falseNegativeRate*100)
//	}
//}

//func TestQuotientFilterFalsePositives(t *testing.T) {
//	const logSize = 8 // 2^8 = 256 slots
//	qf := New(logSize)
//	capacity := 1 << logSize
//
//	// Generate random numbers for insertion
//	rand.Seed(time.Now().UnixNano())
//	numbers := make(map[uint64]bool)
//	for len(numbers) < capacity {
//		numbers[rand.Uint64()] = true
//	}
//
//	for num := range numbers {
//		_ = qf.Insert(uint64ToBytes(num))
//	}
//
//	falsePositives := 0
//	testsCount := 10000
//	for i := 0; i < testsCount; i++ {
//		num := rand.Uint64()
//		if !numbers[num] {
//			exists, _ := qf.Exists(uint64ToBytes(num))
//			if exists {
//				falsePositives++
//			}
//		}
//	}
//	falsePositiveRate := float64(falsePositives) / float64(testsCount)
//	t.Logf("False positive rate: %.4f", falsePositiveRate)
//	// Optionally assert that the false positive rate is within expected bounds
//	if falsePositiveRate > 0.01 { // Example threshold, adjust as needed
//		t.Errorf("False positive rate too high: %.4f", falsePositiveRate)
//	}
//}

//func TestQuotientFilterOverflow(t *testing.T) {
//	const logSize = 8 // 2^8 = 256 slots
//	qf := New(logSize)
//	capacity := 1 << logSize
//
//	// Generate random numbers for insertion
//	rand.Seed(time.Now().UnixNano())
//	numbers := make(map[uint64]bool)
//	for len(numbers) < capacity {
//		numbers[rand.Uint64()] = true
//	}
//
//	for num := range numbers {
//		_ = qf.Insert(uint64ToBytes(num))
//	}
//
//	extraInsertions := capacity / 4 // Try inserting 25% more items
//	extraInsertionFailures := 0
//	for i := 0; i < extraInsertions; i++ {
//		num := rand.Uint64()
//		err := qf.Insert(uint64ToBytes(num))
//		if err != nil {
//			extraInsertionFailures++
//		} else {
//			numbers[num] = true
//		}
//	}
//
//	finalCount := qf.Count()
//	t.Logf("Final count after extra insertions: %d", finalCount)
//
//	falseNegatives := 0
//	for num := range numbers {
//		exists, _ := qf.Exists(uint64ToBytes(num))
//		if !exists {
//			falseNegatives++
//		}
//	}
//	falseNegativeRate := float64(falseNegatives) / float64(len(numbers))
//	t.Logf("Final false negative rate: %.4f", falseNegativeRate)
//
//	if falseNegativeRate > 0.01 { // Allow up to 1% false negative rate
//		t.Errorf("False negative rate too high: %.4f", falseNegativeRate)
//	}
//}

func TestQuotientFilterEdgeCases(t *testing.T) {
	qf := New(10)

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

func BenchmarkQuotientFilterLookup(b *testing.B) {
	qf := New(logSize)

	b.Log("Generating random numbers...")
	numbers := generateRandomNumbers(numItems)

	b.Log("Inserting numbers into the filter...")
	for _, num := range numbers {
		qf.Insert(num)
	}

	b.Log("Generating lookup numbers...")
	lookupNumbers := make([][]byte, benchLookup)
	for i := range lookupNumbers {
		if i < benchLookup/2 {
			lookupNumbers[i] = numbers[rand.Intn(len(numbers))]
		} else {
			lookupNumbers[i] = uint64ToBytes(rand.Uint64())
		}
	}

	b.ResetTimer()
	b.Log("Starting benchmark...")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			num := lookupNumbers[rand.Intn(len(lookupNumbers))]
			qf.Exists(num)
		}
	})
}
