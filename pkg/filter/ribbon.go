package filter

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
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
	built bool
}

type ribbonSnapshot struct {
	M     uint32
	W     uint32
	Seed  uint64
	Cells []uint16
	Built bool
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

	// Текущее elimination хранит строку в локальной uint64-маске.
	// При XOR после выравнивания двух окон нужен диапазон до (2*w - 1) бит.
	// Чтобы избежать потери битов в uint64, лучше поставить лимит окна 32: 2*w - 1 <= 64 => w <= 32.
	maxRibbonWindow uint32 = 32
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
	return ^uint16(0)
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
	if w == 0 || w > maxRibbonWindow {
		return nil, errors.New("w must be in range [1..32]")
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

func (rf *RibbonFilter) BuildWithRetriesFromKeyStream(stream func(func([]byte) bool) error, maxAttempts uint32) error {
	if maxAttempts == 0 {
		return errors.New("maxAttempts must be > 0")
	}
	if stream == nil {
		return errors.New("key stream must not be nil")
	}

	baseSeed := rf.seed
	for attempt := uint32(0); attempt < maxAttempts; attempt++ {
		rf.seed = baseSeed + uint64(attempt)
		if err := rf.BuildFromKeyStream(stream); err == nil {
			return nil
		}
	}

	rf.seed = baseSeed
	return errors.New("failed to build ribbon filter after retries")
}

func (rf *RibbonFilter) BuildFromKeyStream(stream func(func([]byte) bool) error) error {
	if stream == nil {
		return errors.New("key stream must not be nil")
	}

	// Защита для ребилда: пока новая сборка не закончилась успешно,
	// фильтр считается неготовым, а старые values в cells очищаются.
	rf.built = false
	for i := range rf.cells {
		rf.cells[i] = 0
	}

	// План сборки:
	// 1) Для каждого ключа считаем row (start/mask/fingerprint).
	// 2) Сразу делаем elimination: строим pivot-уравнения и сокращаем новые строки через XOR.
	// 3) Делаем back substitution: идем по колонкам справа налево и восстанавливаем rf.cells.
	//
	// Важно: rows не буферизуем отдельным слайсом, чтобы не держать в памяти
	// дубликат данных для больших наборов ключей.

	// pivots[i] хранит одно pivot-уравнение, где cells[i] — ведущая переменная.
	// Когда в новом уравнении ведущая колонка тоже i, делаем XOR с pivots[i],
	// чтобы убрать cells[i] и перейти к следующей ведущей колонке.
	// Пример:
	//   cells[2] XOR cells[3] = 1   // pivot для cells[2]
	//   cells[2] XOR cells[5] = 0   // новое уравнение с тем же lead
	// XOR --------------------------------------------
	//   cells[3] XOR cells[5] = 1
	pivots := make([]*row, rf.m)

	// Gaussian elimination над GF(2)
	processed := false
	var buildErr error
	err := stream(func(item []byte) bool {
		processed = true
		cur := rf.makeRow(item)

		// Внутри одного sourceRow делаем elimination, пока уравнение не:
		// 1) станет новым pivot, или
		// 2) занулится полностью.
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

		// если mask == 0 (все переменные сократились в результате XOR)
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
			buildErr = errors.New("build failed: inconsistent XOR system; increase extraCells or change seed")
			return false
		}

		return true
	})
	if err != nil {
		return err
	}
	if !processed {
		return errors.New("items must not be empty")
	}
	if buildErr != nil {
		return buildErr
	}

	// Обратная подстановка после elimination.
	// Идем справа налево, потому что уравнение для cells[col] может
	// ссылаться на cells с бОльшими индексами, а они уже должны быть посчитаны.
	// Если pivot для колонки нет, переменная свободная: выбираем значение 0.
	for col := int(rf.m) - 1; col >= 0; col-- {
		pivot := pivots[col]
		if pivot == nil {
			rf.cells[col] = 0
			continue
		}

		// Уравнение pivot имеет вид:
		//   cells[col] XOR otherCells = fingerprint
		// => cells[col] = fingerprint XOR otherCells
		cellValue := pivot.fingerprint

		localMask := pivot.mask
		base := pivot.start // начало локального окна для этой строки

		// Перебираем только установленные биты mask (только реально участвующие cells).
		for localMask != 0 {
			bitPos := bits.TrailingZeros64(localMask) // индекс самого младшего установленного бита (1) в localMask
			globalCol := base + uint32(bitPos)        // локальная позиция -> индекс в rf.cells

			// Саму решаемую переменную cells[col] не добавляем.
			if int(globalCol) != col {
				cellValue ^= rf.cells[globalCol] // cellValue = cellValue ^ rf.cells[globalCol]
			}

			// Снимаем обработанный младший установленный бит.
			localMask &= localMask - 1
		}

		rf.cells[col] = cellValue & rf.fpMask()
	}

	rf.built = true
	return nil
}

// Contains проверяет ключ через его XOR-уравнение.
//
// Результат:
// true  => возможно есть (возможны ложноположительные срабатывания)
// false => точно нет
func (rf *RibbonFilter) Contains(item []byte) bool {
	if !rf.built {
		return false
	}

	// По ключу считаем start/mask/fingerprint (те же правила, что и в Build).
	start := rf.start(item)
	mask := rf.mask(item)
	fp := rf.fingerprint(item)

	var acc uint16 = 0

	for i := uint32(0); i < rf.w; i++ {
		// сдвиг маски вправо, через & оставляем только младший бит, если он не 0, значит включен
		if ((mask >> i) & 1) != 0 {
			// XOR'им значения rf.cells[start+i] только для битов i, где mask имеет 1.
			acc ^= rf.cells[start+i] // на этом этапе все cells должны быть известны (сокращены в уравнениях XOR + восстановлены)
		}
	}

	// берем fp из посчитанного xor
	acc &= rf.fpMask()
	// Сравниваем полученный XOR с fingerprint.
	return acc == fp
}

// leadingColumn определяет индекс первой cell в окне
func (r row) leadingColumn() uint32 {
	return r.start + uint32(bits.TrailingZeros64(r.mask))
}

// xorRows вычисляет (cur XOR pivot) как уравнения над GF(2).
//
// Важно: bit k в mask означает не глобальную колонку k, а cells[start+k].
// Поэтому перед XOR pivot.mask надо выровнять относительно cur.start.
// После выравнивания совпадающие глобальные cells[i] окажутся в одном бите
// и корректно сократятся (x XOR x = 0).
func xorRows(cur row, pivot row) row {
	if cur.mask == 0 {
		return pivot
	}
	if pivot.mask == 0 {
		return cur
	}

	// Сдвиг между локальными окнами: переводим pivot.mask в координаты cur.start.
	// Это нужно не для "сравнения окон", а для XOR уравнений над ГЛОБАЛЬНЫМИ cells[i]:
	// после выравнивания одинаковые глобальные ячейки попадают в один и тот же бит
	// и корректно сокращаются (x XOR x = 0).
	//
	// Почему пропускаем очень большие сдвиги (shift >= 64):
	// 1) маска шириной uint64 и поэтому у нас 64  позиций;
	// 2) в elimination мы XOR'им строку только с pivot той же ведущей глобальной колонки,
	//    поэтому такие сдвиги по факту не ожидаются для валидных row (при w <= 64),
	//    но ветки оставлены как безопасная защита.
	shift := int(pivot.start) - int(cur.start)

	var aligned uint64
	switch {
	case shift >= 64:
		// После сдвига на >64бита (в одном окне ячейка в начале, в другом в конце), соответственно
		// все биты выходят за uint64-окно [0..63] и общих позиций в окнах нет.
		aligned = 0
	case shift >= 0:
		aligned = pivot.mask << shift
	case shift <= -64:
		// аналогично для сдвига вправо на 64+.
		aligned = 0
	default:
		aligned = pivot.mask >> (-shift)
	}

	// XOR с aligned = 0 ничего не меняет: x ^ 0 = x.
	// cur.mask:        0b001011
	// aligned:         0b001010
	// новая cur.mask:  0b000001
	cur.mask ^= aligned
	cur.fingerprint ^= pivot.fingerprint

	if cur.mask == 0 {
		cur.start = 0
		return cur
	}

	// Нормализация: убираем хвостовые нули mask и сдвигаем start,
	// чтобы первый установленный бит снова был в позиции 0.
	// и чтобы проход был только по установленным битам
	// cur.mask   = 0b001010
	tz := bits.TrailingZeros64(cur.mask)
	//  cur.start += 1
	cur.start += uint32(tz)
	// cur.mask   = 0b001010 -> 0b000101
	cur.mask >>= tz

	return cur
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

func (rf *RibbonFilter) Serialize(w io.Writer) error {
	snapshot := ribbonSnapshot{
		M:     rf.m,
		W:     rf.w,
		Seed:  rf.seed,
		Cells: append([]uint16(nil), rf.cells...),
		Built: rf.built,
	}

	if err := gob.NewEncoder(w).Encode(snapshot); err != nil {
		return fmt.Errorf("ribbon: serialize: %w", err)
	}

	return nil
}

func LoadRibbonFilter(r io.Reader) (*RibbonFilter, error) {
	var snap ribbonSnapshot
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("ribbon: load: %w", err)
	}

	if snap.M == 0 {
		return nil, errors.New("ribbon: load: invalid m")
	}
	if snap.W == 0 || snap.W > maxRibbonWindow {
		return nil, errors.New("ribbon: load: invalid w")
	}
	if len(snap.Cells) != int(snap.M) {
		return nil, errors.New("ribbon: load: invalid cells length")
	}

	rf := &RibbonFilter{
		m:     snap.M,
		w:     snap.W,
		seed:  snap.Seed,
		cells: append([]uint16(nil), snap.Cells...),
		built: snap.Built,
	}

	return rf, nil
}
