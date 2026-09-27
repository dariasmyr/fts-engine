package fts

type Pipeline interface {
	Process(text string) []string
}
