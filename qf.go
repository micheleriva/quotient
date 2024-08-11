package main

import (
	"fmt"
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
}

func New(logSize uint) *QuotientFilter {
	size := uint64(1) << logSize
	return &QuotientFilter{
		data:     make([]uint64, size),
		mask:     size - 1,
		quotient: logSize,
	}
}

func (qf *QuotientFilter) Insert(data []byte) error {
	qf.mu.Lock()
	defer qf.mu.Unlock()

	item := Murmurhash3(data, 0)
	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		qf.setOccupied(quotient)
		qf.setRunStart(quotient)
		qf.setRunEnd(quotient)
		qf.setRemainder(quotient, remainder)
		return nil
	}

	slot := qf.findSlot(quotient)

	initialSlot := slot
	for {
		if qf.getRemainder(slot) == remainder {
			return nil
		}
		if qf.isRunEnd(slot) || slot == initialSlot {
			break
		}
		slot = (slot + 1) & qf.mask
	}

	return qf.insertRemainder(slot, remainder, quotient)
}

func (qf *QuotientFilter) Exists(data []byte) (bool, time.Duration) {
	startTime := time.Now()

	qf.mu.Lock()
	defer qf.mu.Unlock()

	item := Murmurhash3(data, 0)
	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		return false, time.Since(startTime)
	}

	slot := qf.findSlot(quotient)
	initialSlot := slot
	for {
		if qf.getRemainder(slot) == remainder {
			return true, time.Since(startTime)
		}
		if qf.isRunEnd(slot) || slot == initialSlot {
			return false, time.Since(startTime)
		}
		slot = (slot + 1) & qf.mask
	}
}

func (qf *QuotientFilter) Count() int {
	count := 0
	for i := uint64(0); i < uint64(len(qf.data)); i++ {
		if qf.isOccupied(i) {
			count++
		}
	}
	return count
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
		if qf.isRunStart(slot) && !qf.isShifted(slot) {
			break
		}
		slot = (slot - 1) & qf.mask
	}
	return slot
}

func (qf *QuotientFilter) insertRemainder(slot uint64, remainder uint64, quotient uint64) error {
	err := qf.shiftRight(slot)
	if err != nil {
		return err
	}

	qf.setOccupied(quotient)
	if slot != quotient {
		qf.setShifted(slot)
	}
	if slot == quotient || !qf.isRunStart(slot) {
		qf.setRunStart(slot)
	}
	qf.setRemainder(slot, remainder)

	nextSlot := (slot + 1) & qf.mask
	if !qf.isOccupied(nextSlot) || qf.isRunStart(nextSlot) {
		qf.setRunEnd(slot)
	} else {
		qf.clearRunEnd(slot)
	}

	return nil
}

func (qf *QuotientFilter) clearRunEnd(index uint64) {
	qf.data[index] &= ^uint64(runEnd)
}

func (qf *QuotientFilter) shiftRight(slot uint64) error {
	if qf.isFull() {
		return fmt.Errorf("filter is full")
	}

	currSlot := slot
	for {
		nextSlot := (currSlot + 1) & qf.mask
		if !qf.isOccupied(nextSlot) {
			break
		}
		currSlot = nextSlot
	}

	for currSlot != slot {
		prevSlot := (currSlot - 1) & qf.mask
		qf.data[currSlot] = qf.data[prevSlot]
		qf.setShifted(currSlot)
		currSlot = prevSlot
	}

	return nil
}

func (qf *QuotientFilter) isFull() bool {
	for i := uint64(0); i < uint64(len(qf.data)); i++ {
		if !qf.isOccupied(i) {
			return false
		}
	}
	return true
}
