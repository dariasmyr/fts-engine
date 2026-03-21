package filter

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math/bits"
)

// RibbonFilter собирается статически после того как известны все ключи
// (в отличии от BloomFilter, который поддерживает параллельную чтение по время сборки),
// для каждого ключа собирается окно из ячеек cells,
// которые собраются в XOR уравнение, которое равно куску хеша (fingerpint) ключа
// cells[0] XOR cells[1] = fingerpint
type RibbonFilter struct {
	m     uint32 // чисто ячеек
	w     uint32 // ширина локального окна
	seed  uint64
	cells []uint16 // набор значений которые мы хотим XOR'ить, чтобы получить fingerprint
}

// в row будет все необходимое для XOR
// Ribbon filter при добавлении каждого ключа определяет,
// какие cells в рамках локального окна должны участвовать в XOR уравнении и отдавать fingerprint
type row struct {
	start       uint32 // индекс cells, с которого начинается локальное окно (start = нижние N хеша ключа)
	mask        uint64 // определяет, какие именно cells в диапазоне [start, start+w) будут участвовать в XOR
	fingerprint uint16 // результат XOR (определенные cells по маске)
}

// специально разные константы для хеширования
// (без них хеши для start, mask, fp будут коррелироваться и будут зависимыми -> будет выше шанс ложно положительных вызовов)
const (
	startSalt uint64 = 0x9e3779b97f4a7c15
	maskSalt  uint64 = 0xc2b2ae3d27d4eb4f
	fpSalt    uint64 = 0x165667b19e3779f9
)

// makeRow запечатывает все необходимое для XOR уравнения
// Берем ключ (item):
// 1. хешируем - получаем индекс cells[start]
// 2. хешим еще раз - берем отрезок, получаем mask (01101)
// 3. хешим еще раз - берем отрезок, получаем fp (5)
// Далее эти переменные используются для построения уравнение XOR
// В нем участвуют только те cells внутри окна, индекс которых совпадает с включенными битами в маске
// mask (01101)
// cells[0,1,2,3]
// cells[0] XOR cells[2] XOR cells[3] = 5
func (rf *RibbonFilter) makeRow(item []byte) row {
	return row{
		start:       rf.start(item),
		mask:        rf.mask(item),
		fingerprint: rf.fingerprint(item),
	}
}

// start возвращает начало локального окна
// Important:
// start is NOT hash % m
// start is hash % (m - w + 1)
// so the whole window fits into cells[].
func (rf *RibbonFilter) start(item []byte) uint32 {
	limit := rf.m - rf.w + 1 //лимит должен быть равен длине cells - длина окна
	h := rf.hashWithSalt(item, startSalt)
	return uint32(h % uint64(limit)) // модуль хеша ключа от лимита
}

// mask = обычная bitmask длины w для ключа
// 1 бит означает, что элемент cells из локального окна будет участвовать в XOR
func (rf *RibbonFilter) mask(item []byte) uint64 {
	h := rf.hashWithSalt(item, maskSalt)

	var mask uint64
	if rf.w == 64 {
		mask = h
	} else {
		mask = h & ((uint64(1) << rf.w) - 1) // битовая маска младших w бит.
	}

	// Защита на случай, если mask = 0
	if mask == 0 {
		mask = 1
	}

	return mask
}

// fingerprint = нижние 8 бит хеша
func (rf *RibbonFilter) fingerprint(item []byte) uint16 {
	h := rf.hashWithSalt(item, fpSalt)
	return uint16(h) & rf.fpMask()
}

func (rf *RibbonFilter) fpMask() uint16 {
	return (uint16(1) << 16) - 1
}

// expectedItems: ожидаемое количество ключей
// extraCells: небольшой запас (may be expectedItems/4 or expectedItems/3
// extraCells необходимы, чтобы уменьшить вероятность попадания одинаковых cells в XOR с разным fp
// Пример:
// cells[5] XOR cells[8] = 1
// cells[5] XOR cells[8] = 0
// Эти два уравнения одновременно XOR-нуть нельзя,
// так как одинаковые значения схлопнутся и получится 1 = 0 -> билд упадет

// Поэтому количество cells должно быть слегка больше количества ожидаемых ключей, чтобы было меньше попаданий в одни и те же cells
// Это не избавляет от шанса получить противоречивое уравнение, но снижает вероятность
// Можно было сортировать cells, но этот вариант хитрее
func NewRibbonFilter(expectedItems uint32, extraCells uint32, w uint32, seed uint64) (*RibbonFilter, error) {
	if expectedItems == 0 {
		return nil, errors.New("expectedItems must be > 0")
	}
	if w == 0 || w > 64 {
		return nil, errors.New("w must be in range [1..64]")
	}

	// Количество cells должно быть слегка выше количества expectedItems и не меньше минимального размера окна
	m := expectedItems + extraCells + w

	return &RibbonFilter{
		m:     m,
		w:     w,
		seed:  seed,
		cells: make([]uint16, m),
	}, nil
}

// Build собирает весь сет фильтра по всем ключам за раз
func (rf *RibbonFilter) Build(items [][]byte) error {
	if len(items) == 0 {
		return errors.New("items must not be empty")
	}

	// Распаковка слайса строк на записи для XOR
	// Тут возможна оптимизация, например если известен размер ключа (например 16 байт)
	// вместо [][]byte дать плоский массив [16]byte
	rows := make([]row, 0, len(items))
	for _, item := range items {
		rows = append(rows, rf.makeRow(item))
	}

	// pivots[i] хранит одно pivot-уравнение, в котором cells[i] является ведущей переменной
	// Это уравнение используется, чтобы исключать одинаковые cells[i] из новых уравнений
	// cells[2] XOR cells[3] = 1 // pivot для cells[2]
	// если мы наткнемся на еще одно уравнение где первым идет cells[2]
	// cells[2] XOR cells[5] = 0  // новое уравнение с тем же lead
	// то cells[2] сокращаем:
	// cells[2] XOR cells[3] = 1
	// cells[2] XOR cells[3] = 0
	// и получаем:
	// cells[3] XOR cells[5] = 1
	pivots := make([]*row, rf.m)

	// Gaussian elimination над GF(2)
	for _, sourceRow := range rows {
		cur := sourceRow

		for cur.mask != 0 {
			leadCol := cur.leadingColumn()

			if pivots[leadCol] == nil {
				rowCopy := cur
				pivots[leadCol] = &rowCopy
				break
			}

			cur = xorRows(cur, *pivots[leadCol])
		}

		// То, о чем мы говорили ранее про одинаковые cells, которые внури XOR дают разный fp
		// Для этого надо вспомнить изначально, как строится наше уравнение
		// Мы знаем fp и знаем, что fp получается в результате XOR с cells
		// какие cells - мы не знаем - это определяется по маске в локальном окне

		// если маска = 0 (пустая, а значит одинаковые cells срезались в результате сокращения Гаусса)
		// то в случае успеха fp должен быть 0
		// cells[2] XOR cells[4] = 1
		// cells[2] XOR cells[4] = 1
		// (cells[2] XOR cells[4]) XOR (cells[2] XOR cells[4]) это то же самое что
		// 1 XOR 1 = 0
		// значит если одинаковые cells в обоих уравнениях должны в итоге давать fp = 0

		// так как мы XORим маски (cells нужных индексов) они могут сократиться = mask становится 0
		// если cur.mask == 0 а cur.fingerprint != 0, значит все cells сократились, но fp не 0 - противоречие
		// значит одни и те же cells дали разный fp
		if cur.mask == 0 && cur.fingerprint != 0 {
			return errors.New("build failed: inconsistent XOR system; increase extraCells or change seed")
		}
	}

	// восстанавливаем значения cells с конца (так как уравнение для cells[col] может зависеть от ячеек с большими индексами).
	// если для ячейки нет pivot-уравнения, она свободная, поэтому ставим 0.
	for col := int(rf.m) - 1; col >= 0; col-- {
		pivot := pivots[col]
		if pivot == nil {
			rf.cells[col] = 0
			continue
		}

		// cell[col] XOR otherCells = fingerprint
		// => cell[col] = fingerprint XOR otherCells
		value := pivot.fingerprint

		localMask := pivot.mask
		base := pivot.start

		for localMask != 0 {
			bitPos := bits.TrailingZeros64(localMask)
			globalCol := base + uint32(bitPos)

			if int(globalCol) != col {
				value ^= rf.cells[globalCol]
			}

			localMask &= localMask - 1
		}

		rf.cells[col] = value & rf.fpMask()
	}

	return nil
}

// Contains проверяет, если ли ключ в сете
// true = возможно есть
// false = точно нет
func (rf *RibbonFilter) Contains(item []byte) bool {
	start := rf.start(item)
	mask := rf.mask(item)
	fp := rf.fingerprint(item)

	var acc uint16 = 0

	for i := uint32(0); i < rf.w; i++ {
		if ((mask >> i) & 1) != 0 {
			acc ^= rf.cells[start+i]
		}
	}

	acc &= rf.fpMask()
	return acc == fp
}

// leadingColumn определяет индекс первой cell в окне
func (r row) leadingColumn() uint32 {
	return r.start + uint32(bits.TrailingZeros64(r.mask))
}

// xorRows производит XOR двух rows в GF(2).
//
// Для сравнения надо выровнять row b to row в глобальных координатах
// XOR masks and XOR right-hand side.
func xorRows(a row, b row) row {
	if a.mask == 0 {
		return b
	}
	if b.mask == 0 {
		return a
	}

	// расстояние b.mask до a.start
	shift := int(b.start) - int(a.start)

	var aligned uint64
	switch {
	case shift >= 64:
		aligned = 0
	case shift >= 0:
		aligned = b.mask << shift
	case shift <= -64:
		aligned = 0
	default:
		aligned = b.mask >> (-shift)
	}

	a.mask ^= aligned
	a.fingerprint ^= b.fingerprint

	if a.mask == 0 {
		a.start = 0
		return a
	}

	// нормализация черех обрезание нулей
	tz := bits.TrailingZeros64(a.mask)
	a.start += uint32(tz)
	a.mask >>= tz

	return a
}

func (rf *RibbonFilter) hashWithSalt(item []byte, salt uint64) uint64 {
	return hash64(item, rf.seed^salt)
}

func hash64(data []byte, seed uint64) uint64 {
	h := fnv.New64a()

	var seedBytes [8]byte
	binary.LittleEndian.PutUint64(seedBytes[:], seed)

	_, _ = h.Write(seedBytes[:])
	_, _ = h.Write(data)

	return h.Sum64()
}
