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
	mu       sync.RWMutex
	count    int
}

func NewQuotientFilter(logSize uint) *QuotientFilter {
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

	if qf.count >= len(qf.data) {
		return fmt.Errorf("filter is full")
	}

	if !qf.isOccupied(quotient) {
		qf.setOccupied(quotient)
		qf.setRemainder(quotient, remainder)
		qf.setRunStart(quotient)
		qf.setRunEnd(quotient)
		qf.count++
		return nil
	}

	slot := qf.findRunEnd(quotient)
	for s := qf.findRunStart(quotient); s != (slot+1)&qf.mask; s = (s + 1) & qf.mask {
		if qf.getRemainder(s) == remainder {
			return nil
		}
	}

	if slot == quotient && qf.getRemainder(slot) > remainder {
		slot = (quotient - 1) & qf.mask
	}

	qf.insertIntoSlot((slot+1)&qf.mask, remainder, quotient)
	return nil
}

func (qf *QuotientFilter) Exists(data []byte) (bool, time.Duration) {
	startTime := time.Now()
	qf.mu.RLock()
	defer qf.mu.RUnlock()

	item := murmur3.Sum64(data)
	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		return false, time.Since(startTime)
	}

	slot := qf.findRunStart(quotient)
	endSlot := qf.findRunEnd(quotient)
	for {
		if qf.getRemainder(slot) == remainder {
			return true, time.Since(startTime)
		}
		if slot == endSlot {
			break
		}
		slot = (slot + 1) & qf.mask
	}
	return false, time.Since(startTime)
}

func (qf *QuotientFilter) Count() int {
	qf.mu.RLock()
	defer qf.mu.RUnlock()
	return qf.count
}

func (qf *QuotientFilter) isOccupied(index uint64) bool {
	return qf.data[index&qf.mask]&occupied != 0
}

func (qf *QuotientFilter) setOccupied(index uint64) {
	qf.data[index&qf.mask] |= occupied
}

func (qf *QuotientFilter) isRunStart(index uint64) bool {
	return qf.data[index&qf.mask]&runStart != 0
}

func (qf *QuotientFilter) setRunStart(index uint64) {
	qf.data[index&qf.mask] |= runStart
}

func (qf *QuotientFilter) isRunEnd(index uint64) bool {
	return qf.data[index&qf.mask]&runEnd != 0
}

func (qf *QuotientFilter) setRunEnd(index uint64) {
	qf.data[index&qf.mask] |= runEnd
}

func (qf *QuotientFilter) isShifted(index uint64) bool {
	return qf.data[index&qf.mask]&shifted != 0
}

func (qf *QuotientFilter) setShifted(index uint64) {
	qf.data[index&qf.mask] |= shifted
}

func (qf *QuotientFilter) getRemainder(index uint64) uint64 {
	return qf.data[index&qf.mask] >> 4
}

func (qf *QuotientFilter) setRemainder(index uint64, remainder uint64) {
	qf.data[index&qf.mask] = (qf.data[index&qf.mask] & 0xF) | (remainder << 4)
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

func (qf *QuotientFilter) findRunStart(quotient uint64) uint64 {
	slot := quotient
	for qf.isShifted(slot) && !qf.isRunStart(slot) {
		slot = (slot - 1) & qf.mask
	}
	return slot
}

func (qf *QuotientFilter) findRunEnd(quotient uint64) uint64 {
	slot := qf.findRunStart(quotient)
	for !qf.isRunEnd(slot) {
		nextSlot := (slot + 1) & qf.mask
		if !qf.isShifted(nextSlot) {
			break
		}
		slot = nextSlot
	}
	return slot
}

func (qf *QuotientFilter) insertIntoSlot(slot uint64, remainder uint64, quotient uint64) {
	var prevRemainder, currRemainder uint64
	var prevMetadata, currMetadata uint64
	originalSlot := slot

	currRemainder = remainder
	currMetadata = occupied
	if slot != quotient {
		currMetadata |= shifted
	}
	if slot == quotient || !qf.isRunStart(slot) {
		currMetadata |= runStart
	}

	for {
		if !qf.isOccupied(slot) {
			qf.setRemainder(slot, currRemainder)
			qf.data[slot] = (qf.data[slot] & ^uint64(0xF)) | currMetadata | runEnd
			qf.count++
			return
		}

		prevRemainder = qf.getRemainder(slot)
		prevMetadata = qf.data[slot] & 0xF

		qf.setRemainder(slot, currRemainder)
		qf.data[slot] = (qf.data[slot] & ^uint64(0xF)) | currMetadata

		currRemainder = prevRemainder
		currMetadata = prevMetadata | shifted

		if qf.isRunEnd(slot) {
			qf.data[slot] &= ^uint64(runEnd)
		}

		slot = (slot + 1) & qf.mask
		if slot == originalSlot {
			panic("Filter is full") // This should never happen if we check capacity before inserting
		}
	}
}
