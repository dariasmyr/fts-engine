package ftse

import (
	"context"
	"fmt"
)

// VerifyAll reads and validates every posting and position payload. Open only
// validates metadata needed for lookup, so callers that require eager artifact
// verification must invoke VerifyAll explicitly.
func (r *Reader) VerifyAll(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("ftse: nil reader")
	}
	if ctx == nil {
		return fmt.Errorf("ftse: nil context")
	}
	for i := range r.terms {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := &r.terms[i]
		if r.formatVersion == blockedFormatVersion {
			if err := r.verifyBlockedEntry(ctx, entry); err != nil {
				return fmt.Errorf("ftse: verify term %q: %w", entry.term, err)
			}
			continue
		}
		if _, err := r.SearchPositional(entry.term); err != nil {
			return fmt.Errorf("ftse: verify term %q: %w", entry.term, err)
		}
	}
	return nil
}
