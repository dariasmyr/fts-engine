package embedding

import "strings"

// Model identifies one immutable embedding vector space and its input contract.
// Clients copy Model at construction, so later changes to the caller's value do
// not change the model bound to a client.
type Model struct {
	ID             string
	Revision       string
	Dimensions     int
	ContextTokens  int
	DocumentPrefix string
	QueryPrefix    string
}

// Validate reports whether the model contains a stable identity and all hard
// capabilities required to validate embedding requests and responses.
func (m Model) Validate() error {
	if strings.TrimSpace(m.ID) == "" || strings.TrimSpace(m.Revision) == "" ||
		m.Dimensions <= 0 || m.ContextTokens <= 0 {
		return ErrInvalidModel
	}
	return nil
}
