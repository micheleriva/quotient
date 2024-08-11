package main

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestMurmurHash3(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		seed     uint32
		expected uint64
	}{
		{
			name:     "Empty string",
			input:    []byte(""),
			seed:     0,
			expected: 0x0000000000000000,
		},
		{
			name:     "Simple string",
			input:    []byte("hello"),
			seed:     0,
			expected: 0xcbd8a7b341bd9b02,
		},
		{
			name:     "Longer string",
			input:    []byte("The quick brown fox jumps over the lazy dog"),
			seed:     0,
			expected: 0xe34bbc7bbc071b6c,
		},
		{
			name:     "Non-zero seed",
			input:    []byte("hello"),
			seed:     42,
			expected: 0xc4b8b3c960af6f08,
		},
		{
			name:     "Binary data",
			input:    []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			seed:     0,
			expected: 0x9ce80ca5ef93bfdc,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Murmurhash3(tt.input, tt.seed)
			if result != tt.expected {
				t.Errorf("murmurhash3(%v, %d) = %x, want %x", tt.input, tt.seed, result, tt.expected)
			}
		})
	}
}
func TestMurmurHash3Properties(t *testing.T) {
	t.Run("Deterministic", func(t *testing.T) {
		input := []byte("test input")
		seed := uint32(0)
		hash1 := Murmurhash3(input, seed)
		hash2 := Murmurhash3(input, seed)
		if hash1 != hash2 {
			t.Errorf("Hash function is not deterministic: %x != %x", hash1, hash2)
		}
	})

	t.Run("Avalanche effect", func(t *testing.T) {
		input1 := []byte("test input")
		input2 := []byte("test inpuT")
		seed := uint32(0)
		hash1 := Murmurhash3(input1, seed)
		hash2 := Murmurhash3(input2, seed)
		if hash1 == hash2 {
			t.Errorf("Hash function doesn't show avalanche effect: %x == %x", hash1, hash2)
		}
	})

	t.Run("Uniformity", func(t *testing.T) {
		buckets := make([]int, 256)
		iterations := 100000
		for i := 0; i < iterations; i++ {
			input := make([]byte, 8)
			binary.LittleEndian.PutUint64(input, uint64(i))
			hash := Murmurhash3(input, 0)
			buckets[hash%256]++
		}

		expected := float64(iterations) / 256
		chiSquare := 0.0
		for _, count := range buckets {
			chiSquare += math.Pow(float64(count)-expected, 2) / expected
		}
		
		if chiSquare > 293.2478 {
			t.Errorf("Distribution doesn't appear uniform. Chi-square: %f", chiSquare)
		}
	})
}
