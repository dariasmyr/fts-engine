package filters

type noopFilter struct{}

func NewNoopFilter() Filter {
	return noopFilter{}
}

func (noopFilter) Add(_ string) {}

func (noopFilter) MightContain(_ string) bool {
	return true
}
