package fnv

import "testing"

func TestSum32a(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{input: "", want: 0x811c9dc5},
		{input: "a", want: 0xe40c292c},
		{input: "foobar", want: 0xbf9cf968},
	}

	for _, tt := range tests {
		if got := Sum32a(tt.input); got != tt.want {
			t.Errorf("Sum32a(%q) = %#x, want %#x", tt.input, got, tt.want)
		}
		if got := Sum32a([]byte(tt.input)); got != tt.want {
			t.Errorf("Sum32a([]byte(%q)) = %#x, want %#x", tt.input, got, tt.want)
		}
	}
}
