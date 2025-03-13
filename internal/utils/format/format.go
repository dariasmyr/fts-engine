package utils

import (
	"fmt"
	"time"
)

// Format the duration into a human-readable string
func FormatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.3fns", float64(d)/float64(time.Nanosecond))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.3fµs", float64(d)/float64(time.Microsecond))
	} else if d < time.Second {
		return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
}
