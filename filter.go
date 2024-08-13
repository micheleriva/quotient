package main

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	occupied = 1 << 0
	runStart = 1 << 1
	runEnd   = 1 << 2
	shifted  = 1 << 3
	stripes  = 16 // Number of stripes for striped locking
)

type QuotientFilter struct {
	data     []uint64
	mask     uint64
	quotient uint
	locks    [stripes]sync.RWMutex
	count    atomic.Int64
}

func NewQuotientFilter(logSize uint) *QuotientFilter {
	size := uint64(1) << logSize
	return &QuotientFilter{
		data:     make([]uint64, size),
		mask:     size - 1,
		quotient: logSize,
	}
}

func (qf *QuotientFilter) Insert(data []byte) error {
	quotient, remainder := qf.hash(data)

	if qf.count.Load() >= int64(len(qf.data)) {
		return fmt.Errorf("filter is full")
	}

	qf.lockStripe(quotient)
	defer qf.unlockStripe(quotient)

	exists := qf.existsUnsafe(quotient, remainder)
	if exists {
		return nil
	}

	slot := qf.findSlot(quotient)
	qf.insertIntoSlot(slot, remainder, quotient)
	qf.count.Add(1)
	return nil
}

func (qf *QuotientFilter) Exists(data []byte) (bool, time.Duration) {
	startTime := time.Now()
	quotient, remainder := qf.hash(data)

	qf.rLockStripe(quotient)
	defer qf.rUnlockStripe(quotient)

	if !qf.isOccupied(quotient) {
		return false, time.Since(startTime)
	}

	runStart := qf.findRunStart(quotient)
	runEnd := qf.findRunEnd(quotient)

	for slot := runStart; ; slot = (slot + 1) & qf.mask {
		if qf.getRemainder(slot) == remainder {
			return true, time.Since(startTime)
		}
		if slot == runEnd {
			break
		}
	}

	return false, time.Since(startTime)
}

func (qf *QuotientFilter) Remove(data []byte) bool {
	quotient, remainder := qf.hash(data)

	qf.lockStripe(quotient)
	defer qf.unlockStripe(quotient)

	if !qf.isOccupied(quotient) {
		return false
	}

	runStart := qf.findRunStart(quotient)
	runEnd := qf.findRunEnd(quotient)

	for slot := runStart; ; slot = (slot + 1) & qf.mask {
		if qf.getRemainder(slot) == remainder {
			qf.removeAt(slot, quotient, runStart, runEnd)
			qf.count.Add(-1)
			return true
		}
		if slot == runEnd {
			break
		}
	}

	return false
}

func (qf *QuotientFilter) Count() int {
	return int(qf.count.Load())
}

func (qf *QuotientFilter) existsUnsafe(quotient, remainder uint64) bool {
	if !qf.isOccupied(quotient) {
		return false
	}

	runStart := qf.findRunStart(quotient)
	runEnd := qf.findRunEnd(quotient)

	for slot := runStart; ; slot = (slot + 1) & qf.mask {
		if qf.getRemainder(slot) == remainder {
			return true
		}
		if slot == runEnd {
			break
		}
	}

	return false
}

func (qf *QuotientFilter) hash(data []byte) (quotient uint64, remainder uint64) {
	h := fnv.New64a()
	h.Write(data)
	hashValue := h.Sum64()
	quotient = hashValue & qf.mask
	remainder = hashValue >> qf.quotient
	return
}

func (qf *QuotientFilter) removeAt(slot, quotient, runStart, runEnd uint64) {
	isOnlyItemInRun := runStart == runEnd
	isFirstItemInRun := slot == runStart
	isLastItemInRun := slot == runEnd

	if isOnlyItemInRun {
		qf.clearOccupied(quotient)
		qf.clearRunStart(slot)
		qf.clearRunEnd(slot)
		qf.clearRemainder(slot)
		qf.clearShifted(slot)
	} else {
		qf.shiftLeft(slot, runEnd)

		if isFirstItemInRun {
			newRunStart := (slot + 1) & qf.mask
			qf.clearRunStart(slot)
			qf.setRunStart(newRunStart)
			if newRunStart != quotient {
				qf.setShifted(newRunStart)
			}
		}
		if isLastItemInRun {
			newRunEnd := (runEnd - 1) & qf.mask
			qf.clearRunEnd(runEnd)
			qf.setRunEnd(newRunEnd)
		}
	}

	qf.updateMetadataAfterRemoval(quotient)
}

func (qf *QuotientFilter) updateMetadataAfterRemoval(quotient uint64) {
	if !qf.isOccupied(quotient) {
		return
	}

	runStart := qf.findRunStart(quotient)
	runEnd := qf.findRunEnd(quotient)

	if runStart == runEnd {
		qf.setRunStart(runStart)
		qf.setRunEnd(runStart)
		qf.clearShifted(runStart)
	} else {
		qf.setRunStart(runStart)
		qf.setRunEnd(runEnd)
		for slot := (runStart + 1) & qf.mask; slot != (runEnd+1)&qf.mask; slot = (slot + 1) & qf.mask {
			qf.setShifted(slot)
		}
	}

	if qf.getRemainder(runStart) == 0 && !qf.isShifted(runStart) {
		qf.clearOccupied(quotient)
	}
}

func (qf *QuotientFilter) isOccupied(index uint64) bool {
	return atomic.LoadUint64(&qf.data[index&qf.mask])&occupied != 0
}

func (qf *QuotientFilter) setOccupied(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old | occupied
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) clearOccupied(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old &^ occupied
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) clearRunStart(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old &^ uint64(runStart)
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) clearRunEnd(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old &^ uint64(runEnd)
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) isRunStart(index uint64) bool {
	return atomic.LoadUint64(&qf.data[index&qf.mask])&runStart != 0
}

func (qf *QuotientFilter) setRunStart(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old | runStart
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) isRunEnd(index uint64) bool {
	return atomic.LoadUint64(&qf.data[index&qf.mask])&runEnd != 0
}

func (qf *QuotientFilter) setRunEnd(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old | runEnd
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) isShifted(index uint64) bool {
	return atomic.LoadUint64(&qf.data[index&qf.mask])&shifted != 0
}

func (qf *QuotientFilter) setShifted(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old | shifted
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) getRemainder(index uint64) uint64 {
	return atomic.LoadUint64(&qf.data[index&qf.mask]) >> 4
}

func (qf *QuotientFilter) setRemainder(index uint64, remainder uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := (old & 0xF) | (remainder << 4)
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) findSlot(quotient uint64) uint64 {
	slot := quotient
	for qf.isOccupied(slot) {
		if !qf.isShifted(slot) {
			break
		}
		slot = (slot + 1) & qf.mask
	}
	return slot
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

	qf.count.Add(1)
	return nil
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
		atomic.StoreUint64(&qf.data[currentSlot], atomic.LoadUint64(&qf.data[prevSlot]))
		qf.setShifted(currentSlot)
		currentSlot = prevSlot
	}

	qf.clearSlot(slot) // Clear the original slot
	return nil
}

func (qf *QuotientFilter) clearSlot(index uint64) {
	atomic.StoreUint64(&qf.data[index&qf.mask], 0)
}

func (qf *QuotientFilter) clearRemainder(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old & 0xF // Clear all but the lowest 4 bits (metadata)
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) clearShifted(index uint64) {
	for {
		old := atomic.LoadUint64(&qf.data[index&qf.mask])
		new := old &^ uint64(shifted)
		if atomic.CompareAndSwapUint64(&qf.data[index&qf.mask], old, new) {
			return
		}
	}
}

func (qf *QuotientFilter) shiftLeft(start, end uint64) {
	current := start
	next := (start + 1) & qf.mask

	for current != end {
		atomic.StoreUint64(&qf.data[current], atomic.LoadUint64(&qf.data[next]))
		current = next
		next = (next + 1) & qf.mask
	}

	qf.clearSlot(end)
}

func (qf *QuotientFilter) isFull() bool {
	return qf.count.Load() >= int64(len(qf.data))
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
			atomic.StoreUint64(&qf.data[slot], (qf.data[slot]&^uint64(0xF))|currMetadata|runEnd)
			return
		}

		prevRemainder = qf.getRemainder(slot)
		prevMetadata = atomic.LoadUint64(&qf.data[slot]) & 0xF

		qf.setRemainder(slot, currRemainder)
		atomic.StoreUint64(&qf.data[slot], (qf.data[slot]&^uint64(0xF))|currMetadata)

		currRemainder = prevRemainder
		currMetadata = prevMetadata | shifted

		if qf.isRunEnd(slot) {
			qf.clearRunEnd(slot)
			currMetadata |= runEnd
		}

		slot = (slot + 1) & qf.mask
		if slot == originalSlot {
			panic("Filter is full")
		}
	}
}

func (qf *QuotientFilter) lockStripe(index uint64) {
	qf.locks[index%stripes].Lock()
}

func (qf *QuotientFilter) unlockStripe(index uint64) {
	qf.locks[index%stripes].Unlock()
}

func (qf *QuotientFilter) rLockStripe(index uint64) {
	qf.locks[index%stripes].RLock()
}

func (qf *QuotientFilter) rUnlockStripe(index uint64) {
	qf.locks[index%stripes].RUnlock()
}
