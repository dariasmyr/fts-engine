package fts

import "context"

func defaultDoc(id DocID, content string) Document {
	return Document{
		ID: id,
		Fields: map[string]Field{
			DefaultField: {Value: content},
		},
	}
}

func indexDefaultDoc(ctx context.Context, svc *Service, id DocID, content string) error {
	return svc.Index(ctx, defaultDoc(id, content))
}

func updateDefaultDoc(ctx context.Context, svc *Service, id DocID, content string) error {
	return svc.Update(ctx, defaultDoc(id, content))
}
