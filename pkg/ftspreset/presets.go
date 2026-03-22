package ftspreset

import (
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func English() fts.Option {
	return fts.WithPipeline(textproc.DefaultEnglishPipeline())
}

func Russian() fts.Option {
	return fts.WithPipeline(textproc.DefaultRussianPipeline())
}

func Multilingual() fts.Option {
	return fts.WithPipeline(textproc.DefaultMultilingualPipeline())
}
