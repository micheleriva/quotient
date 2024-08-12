package main

import (
	"github.com/google/uuid"
	"math/rand"
	"testing"
)

func generateRandomUUIDs(n int) [][]byte {
	uuids := make([][]byte, n)
	for i := range uuids {
		uuidBytes, _ := uuid.New().MarshalBinary()
		uuids[i] = uuidBytes
	}
	return uuids
}

func BenchmarkQuotientFilterLookup(b *testing.B) {
	qf := NewQuotientFilter(22) // 2^22, 4,194,304 slots

	b.Log("Generating random UUIDs...")
	uuids := generateRandomUUIDs(numItems)

	b.Log("Inserting UUIDs into the filter...")
	for _, id := range uuids {
		qf.Insert(id)
	}

	b.Log("Generating lookup UUIDs...")
	lookupUUIDs := make([][]byte, benchLookup)
	for i := range lookupUUIDs {
		if i < benchLookup/2 {
			lookupUUIDs[i] = uuids[rand.Intn(len(uuids))]
		} else {
			newUUID, _ := uuid.New().MarshalBinary()
			lookupUUIDs[i] = newUUID
		}
	}

	b.ResetTimer()
	b.Log("Starting benchmark...")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := lookupUUIDs[rand.Intn(len(lookupUUIDs))]
			qf.Exists(id)
		}
	})
}
