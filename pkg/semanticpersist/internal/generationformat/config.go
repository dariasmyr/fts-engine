package generationformat

// Config is the complete static validation configuration for generation files.
type Config struct {
	MaxFileBytes         uint64
	MaxStringBytes       int
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

func (c Config) validateFile() error {
	if c.MaxFileBytes == 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (c Config) validateStrings() error {
	if err := c.validateFile(); err != nil {
		return err
	}
	if c.MaxStringBytes <= 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (c Config) validateManifest() error {
	if err := c.validateStrings(); err != nil {
		return err
	}
	if c.MaxSegments <= 0 {
		return ErrLimitExceeded
	}
	return nil
}

func (c Config) validateState() error {
	if err := c.validateStrings(); err != nil {
		return err
	}
	if c.MaxVectorBytes == 0 || c.MaxDimensions <= 0 || c.MaxVectors <= 0 ||
		c.MaxSegments <= 0 || c.MaxDocuments <= 0 || c.MaxChunksPerDocument <= 0 ||
		c.MaxK <= 0 || c.MaxEfSearch <= 0 || c.MaxVisitLimit <= 0 {
		return ErrLimitExceeded
	}
	return nil
}
