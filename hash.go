package main

import (
	"encoding/binary"
	"math/bits"
)

func Murmurhash3(key []byte, seed uint32) uint64 {
	const (
		c1 = uint64(0x87c37b91114253d5)
		c2 = uint64(0x4cf5ad432745937f)
	)
	length := len(key)
	h1 := uint64(seed)
	h2 := uint64(seed)

	for len(key) >= 16 {
		k1 := binary.LittleEndian.Uint64(key)
		k2 := binary.LittleEndian.Uint64(key[8:])

		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5

		key = key[16:]
	}

	if len(key) > 0 {
		var k1, k2 uint64
		switch len(key) & 15 {
		case 15:
			k2 ^= uint64(key[14]) << 48
			fallthrough
		case 14:
			k2 ^= uint64(key[13]) << 40
			fallthrough
		case 13:
			k2 ^= uint64(key[12]) << 32
			fallthrough
		case 12:
			k2 ^= uint64(key[11]) << 24
			fallthrough
		case 11:
			k2 ^= uint64(key[10]) << 16
			fallthrough
		case 10:
			k2 ^= uint64(key[9]) << 8
			fallthrough
		case 9:
			k2 ^= uint64(key[8])
			k2 *= c2
			k2 = bits.RotateLeft64(k2, 33)
			k2 *= c1
			h2 ^= k2
			fallthrough
		case 8:
			k1 ^= uint64(key[7]) << 56
			fallthrough
		case 7:
			k1 ^= uint64(key[6]) << 48
			fallthrough
		case 6:
			k1 ^= uint64(key[5]) << 40
			fallthrough
		case 5:
			k1 ^= uint64(key[4]) << 32
			fallthrough
		case 4:
			k1 ^= uint64(key[3]) << 24
			fallthrough
		case 3:
			k1 ^= uint64(key[2]) << 16
			fallthrough
		case 2:
			k1 ^= uint64(key[1]) << 8
			fallthrough
		case 1:
			k1 ^= uint64(key[0])
			k1 *= c1
			k1 = bits.RotateLeft64(k1, 31)
			k1 *= c2
			h1 ^= k1
		}
	}

	h1 ^= uint64(length)
	h2 ^= uint64(length)

	h1 += h2
	h2 += h1

	h1 = fmix64(h1)
	h2 = fmix64(h2)

	h1 += h2
	h2 += h1

	return h1
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}
