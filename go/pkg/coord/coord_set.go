package coord

type bitset struct {
	words []uint32
}

const (
	BITS_PER_WORD = 32
)

func newBitset(zoom uint) *bitset {
	// zoom > 16 would overflow on 32bit uint machines. i don't see the need for zoom > 16, so it's easier to rule it out here than to handle it.
	if zoom > 16 {
		panic("Zoom levels > 16 are not currently supported by coord.bitset")
	}

	// the number of bits we need in the array
	num_bits := uint(1) << (2 * zoom)

	// but we can only allocate larger types, so round up to a whole number of them. we'll use 32-bit words, as these should be aligned accesses
	num_words := num_bits / BITS_PER_WORD
	if num_bits % BITS_PER_WORD > 0 {
		num_words += 1
	}

	// note: it _looks_ like slice elements are initialized to zero as per usual, but i don't see that explicitly documented anywhere...
	words := make([]uint32, num_words)
	return &bitset{words}
}

func (b *bitset) Get(idx uint) bool {
	word_idx := idx / BITS_PER_WORD
	word_offset := idx % BITS_PER_WORD
	w := b.words[word_idx]
	return ((w >> word_offset) & 1) == 1
}

func (b *bitset) Set(idx uint, val bool) {
	word_idx := idx / BITS_PER_WORD
	word_offset := idx % BITS_PER_WORD
	w := b.words[word_idx]

	if val {
		w = w | (uint32(1) << word_offset)
	} else {
		w = w &^ (uint32(1) << word_offset)
	}

	b.words[word_idx] = w
}

type CoordSet struct {
	zooms map[uint]*bitset
}

func NewCoordSet() *CoordSet {
	return &CoordSet{make(map[uint]*bitset)}
}

func (s *CoordSet) Get(c Coord) bool {
	b, ok := s.zooms[c.Z]

	if !ok {
		return false
	}

	idx := (c.Y << c.Z) | c.X
	return b.Get(idx)
}

func (s *CoordSet) Set(c Coord, val bool) {
	b, ok := s.zooms[c.Z]

	if !ok {
		b = newBitset(c.Z)
		s.zooms[c.Z] = b
	}

	idx := (c.Y << c.Z) | c.X
	b.Set(idx, val)
}
