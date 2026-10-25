package semanticformat

// FileLimits contains limits shared by the small CRC-protected metadata files.
type FileLimits struct {
	MaxFileBytes   uint64
	MaxStringBytes int
}

// ManifestLimits contains limits used while encoding and decoding manifest files.
type ManifestLimits struct {
	FileLimits
	MaxSegments int
}

// StateLimits contains limits used while encoding and decoding semantic state.
type StateLimits struct {
	FileLimits
	MaxVectorBytes       uint64
	MaxDimensions        int
	MaxVectors           int
	MaxSegments          int
	MaxDocuments         int
	MaxChunksPerDocument int
	MaxK                 int
	MaxEfSearch          int
	MaxVisitLimit        int
}

// VectorLimits contains bounds for the SVEC vector payload format.
type VectorLimits struct {
	MaxDimensions  int
	MaxVectors     int
	MaxVectorBytes uint64
	MaxK           int
}

func (l FileLimits) validateFile() error {
	if l.MaxFileBytes == 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (l FileLimits) validateStrings() error {
	if err := l.validateFile(); err != nil {
		return err
	}
	if l.MaxStringBytes <= 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (l ManifestLimits) validate() error {
	if err := l.FileLimits.validateStrings(); err != nil {
		return err
	}
	if l.MaxSegments <= 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (l StateLimits) validate() error {
	if err := l.FileLimits.validateStrings(); err != nil {
		return err
	}
	if l.MaxVectorBytes == 0 ||
		l.MaxDimensions <= 0 ||
		l.MaxVectors <= 0 ||
		l.MaxSegments <= 0 ||
		l.MaxDocuments <= 0 ||
		l.MaxChunksPerDocument <= 0 ||
		l.MaxK <= 0 ||
		l.MaxEfSearch <= 0 ||
		l.MaxVisitLimit <= 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (l VectorLimits) validate() error {
	if l.MaxDimensions <= 0 || l.MaxVectors <= 0 || l.MaxVectorBytes == 0 || l.MaxK <= 0 {
		return ErrLimitExceeded
	}
	return nil
}
