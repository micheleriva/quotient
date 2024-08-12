package main

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"sync"
	"time"
)

const (
	occupied = 1 << 0
	runStart = 1 << 1
	runEnd   = 1 << 2
	shifted  = 1 << 3
)

type QuotientFilter struct {
	data     []uint64
	mask     uint64
	quotient uint
	mu       sync.Mutex
	count    int
}

func New(logSize uint) *QuotientFilter {
	size := uint64(1) << logSize
	return &QuotientFilter{
		data:     make([]uint64, size),
		mask:     size - 1,
		quotient: logSize,
		count:    0,
	}
}

func (qf *QuotientFilter) Insert(data []byte) error {
	qf.mu.Lock()
	defer qf.mu.Unlock()

	item := murmur3.Sum64(data)
	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		qf.setOccupied(quotient)
		qf.setRunStart(quotient)
		qf.setRunEnd(quotient)
		qf.setRemainder(quotient, remainder)
		qf.count++
		return nil
	}

	slot := qf.findSlot(quotient)
	return qf.insertRemainder(slot, remainder, quotient)
}

func (qf *QuotientFilter) Exists(data []byte) (bool, time.Duration) {
	startTime := time.Now()

	qf.mu.Lock()
	defer qf.mu.Unlock()

	item := murmur3.Sum64(data)
	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		return false, time.Since(startTime)
	}

	slot := qf.findSlot(quotient)
	initialSlot := slot
	for {
		slotRemainder := qf.getRemainder(slot)
		if slotRemainder == remainder {
			return true, time.Since(startTime)
		}
		if qf.isRunEnd(slot) || slot == initialSlot {
			return false, time.Since(startTime)
		}
		slot = (slot + 1) & qf.mask
	}
}

func (qf *QuotientFilter) Count() int {
	qf.mu.Lock()
	defer qf.mu.Unlock()
	return qf.count
}

func (qf *QuotientFilter) isOccupied(index uint64) bool {
	return qf.data[index]&occupied != 0
}

func (qf *QuotientFilter) setOccupied(index uint64) {
	qf.data[index] |= occupied
}

func (qf *QuotientFilter) isRunStart(index uint64) bool {
	return qf.data[index]&runStart != 0
}

func (qf *QuotientFilter) setRunStart(index uint64) {
	qf.data[index] |= runStart
}

func (qf *QuotientFilter) isRunEnd(index uint64) bool {
	return qf.data[index]&runEnd != 0
}

func (qf *QuotientFilter) setRunEnd(index uint64) {
	qf.data[index] |= runEnd
}

func (qf *QuotientFilter) isShifted(index uint64) bool {
	return qf.data[index]&shifted != 0
}

func (qf *QuotientFilter) setShifted(index uint64) {
	qf.data[index] |= shifted
}

func (qf *QuotientFilter) getRemainder(index uint64) uint64 {
	return qf.data[index] >> 4
}

func (qf *QuotientFilter) setRemainder(index uint64, remainder uint64) {
	qf.data[index] = (qf.data[index] & 0xF) | (remainder << 4)
}

func (qf *QuotientFilter) findSlot(quotient uint64) uint64 {
	slot := quotient
	for qf.isOccupied(slot) && (slot == quotient || qf.isShifted(slot)) {
		slot = (slot + 1) & qf.mask
	}
	return (slot - 1) & qf.mask
}

func (qf *QuotientFilter) insertRemainder(slot uint64, remainder uint64, quotient uint64) error {
	if qf.isFull() {
		return fmt.Errorf("filter is full")
	}

	currentSlot := slot
	for {
		if !qf.isOccupied(currentSlot) {
			break
		}
		if qf.getRemainder(currentSlot) == remainder {
			return nil // Item already exists, don't insert or increment count
		}
		if qf.isRunEnd(currentSlot) {
			break
		}
		currentSlot = (currentSlot + 1) & qf.mask
	}

	if currentSlot != slot {
		err := qf.shiftRight(currentSlot)
		if err != nil {
			return err
		}
	}

	qf.setOccupied(currentSlot)
	qf.setRemainder(currentSlot, remainder)

	if currentSlot != quotient {
		qf.setShifted(currentSlot)
	}
	if currentSlot == quotient || !qf.isRunStart(currentSlot) {
		qf.setRunStart(currentSlot)
	}

	nextSlot := (currentSlot + 1) & qf.mask
	if !qf.isOccupied(nextSlot) || qf.isRunStart(nextSlot) {
		qf.setRunEnd(currentSlot)
	} else {
		qf.clearRunEnd((currentSlot - 1) & qf.mask)
	}

	qf.count++
	return nil
}

func (qf *QuotientFilter) clearRunEnd(index uint64) {
	qf.data[index] &= ^uint64(runEnd)
}

func (qf *QuotientFilter) shiftRight(slot uint64) error {
	currentSlot := slot
	for {
		nextSlot := (currentSlot + 1) & qf.mask
		if !qf.isOccupied(nextSlot) {
			break
		}
		currentSlot = nextSlot
	}

	for currentSlot != slot {
		prevSlot := (currentSlot - 1) & qf.mask
		qf.data[currentSlot] = qf.data[prevSlot]
		qf.setShifted(currentSlot)
		currentSlot = prevSlot
	}

	qf.data[slot] = 0 // Clear the original slot
	return nil
}

func (qf *QuotientFilter) isFull() bool {
	return qf.count >= len(qf.data)
}
