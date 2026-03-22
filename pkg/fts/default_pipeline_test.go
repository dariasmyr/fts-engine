package fts

import (
	"reflect"
	"testing"
)

func TestDefaultPipelineProcess(t *testing.T) {
	pipe := defaultPipeline{}

	got := pipe.Process("Hello, Мир 2026!")
	want := []string{"hello", "мир", "2026"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens = %#v, want %#v", got, want)
	}
}
