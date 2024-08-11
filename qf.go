package main

import (
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

func (qf *QuotientFilter) Insert(item uint64) {
	qf.mu.Lock()
	defer qf.mu.Unlock()

	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		qf.setOccupied(quotient)
		qf.setRunStart(quotient)
		qf.setRunEnd(quotient)
		qf.setRemainder(quotient, remainder)
		return
	}

	slot := qf.findSlot(quotient)
	qf.insertRemainder(slot, remainder)
}

func (qf *QuotientFilter) Exists(item uint64) (bool, time.Duration) {
	startTime := time.Now()

	qf.mu.Lock()
	defer qf.mu.Unlock()

	quotient := item & qf.mask
	remainder := item >> qf.quotient

	if !qf.isOccupied(quotient) {
		return false, time.Since(startTime)
	}

	slot := qf.findSlot(quotient)
	for {
		if qf.getRemainder(slot) == remainder {
			return true, time.Since(startTime)
		}
		if qf.isRunEnd(slot) {
			return false, time.Since(startTime)
		}
		slot = (slot + 1) & qf.mask
	}
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
	for qf.isOccupied(slot) && !qf.isRunStart(slot) {
		slot = (slot + 1) & qf.mask
	}
	return slot
}

func (qf *QuotientFilter) insertRemainder(slot uint64, remainder uint64) {
	if qf.isOccupied(slot) {
		qf.shiftRight(slot)
	}

	qf.setOccupied(slot)
	if !qf.isRunStart(slot) {
		qf.setRunStart(slot)
	}
	if !qf.isOccupied((slot + 1) & qf.mask) {
		qf.setRunEnd(slot)
	} else {
		qf.setShifted((slot + 1) & qf.mask)
	}
	qf.setRemainder(slot, remainder)
}

func (qf *QuotientFilter) shiftRight(slot uint64) {
	for qf.isOccupied(slot) {
		next := (slot + 1) & qf.mask
		qf.data[next], qf.data[slot] = qf.data[slot], qf.data[next]
		qf.setShifted(next)
		slot = next
	}
}
