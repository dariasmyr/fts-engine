package fts

type queryFieldScope struct {
	active  bool
	fields  []string
	allowed map[string]struct{}
}

func newQueryFieldScope(fields []string) queryFieldScope {
	scope := queryFieldScope{active: true}
	if len(fields) == 0 {
		scope.fields = []string{}
		scope.allowed = map[string]struct{}{}
		return scope
	}

	scope.fields = make([]string, 0, len(fields))
	scope.allowed = make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == "" {
			continue
		}
		if _, exists := scope.allowed[field]; exists {
			continue
		}
		scope.allowed[field] = struct{}{}
		scope.fields = append(scope.fields, field)
	}
	return scope
}

func (s *Service) resolveScopedFields(explicit string, scope queryFieldScope) []string {
	if explicit != "" {
		if !scope.active {
			return []string{explicit}
		}
		if _, allowed := scope.allowed[explicit]; allowed {
			return []string{explicit}
		}
		return nil
	}
	if scope.active {
		return scope.fields
	}
	return s.fieldNames()
}
