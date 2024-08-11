package main

import (
	"encoding/binary"
	"math/rand"
	"testing"
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
	qf := New(10)

	testItem := uint64ToBytes(12345)
	t.Logf("Inserting first item")
	err := qf.Insert(testItem)
	if err != nil {
		t.Fatalf("Failed to insert first item: %v", err)
	}
	t.Logf("First item inserted")

	t.Logf("Inserting second item")
	err = qf.Insert(testItem)
	if err != nil {
		t.Fatalf("Failed to insert duplicate item: %v", err)
	}
	t.Logf("Second item insertion attempt completed")

	t.Logf("Checking if item exists")
	exists, duration := qf.Exists(testItem)
	t.Logf("Exists check took %v", duration)
	if !exists {
		t.Errorf("Item should exist in the filter after insertion")
	}

	count := qf.Count()
	t.Logf("Filter reports %d items", count)
	if count != 1 {
		t.Errorf("Expected 1 item in the filter, but found %d", count)
	}
}

//func TestQuotientFilterCapacity(t *testing.T) {
//	qf := New(4) // 16 slots
//
//	for i := uint64(0); i < 16; i++ {
//		err := qf.Insert(uint64ToBytes(i))
//		if err != nil {
//			t.Errorf("Failed to insert item %d: %v", i, err)
//		}
//	}
//
//	count := qf.Count()
//	t.Logf("Filter reports %d occupied slots", count)
//
//	for i := uint64(0); i < 16; i++ {
//		exists, _ := qf.Exists(uint64ToBytes(i))
//		if !exists {
//			t.Errorf("Item %d should exist in the filter, but doesn't", i)
//		}
//	}
//
//	err := qf.Insert(uint64ToBytes(16))
//	if err == nil {
//		t.Errorf("Expected an error when inserting beyond capacity")
//	} else {
//		t.Logf("Received expected error when inserting beyond capacity: %v", err)
//	}
//
//	falseNegatives := 0
//	for i := uint64(0); i <= 16; i++ {
//		exists, _ := qf.Exists(uint64ToBytes(i))
//		if !exists && i < 16 {
//			falseNegatives++
//		}
//	}
//
//	if falseNegatives > 0 {
//		t.Errorf("Filter has %d false negatives when operating at capacity", falseNegatives)
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
