package fts

import "fmt"

type processedFieldGroup struct {
	fields []string
	tokens []string
}

func (s *Service) processFields(fields []string, text string) ([]processedFieldGroup, error) {
	type fieldPipeline struct {
		field    string
		pipeline Pipeline
	}

	seenFields := make(map[string]struct{}, len(fields))
	fieldPipelines := make([]fieldPipeline, 0, len(fields))
	for _, field := range fields {
		if field == "" {
			continue
		}
		if _, seen := seenFields[field]; seen {
			continue
		}
		seenFields[field] = struct{}{}
		fieldPipelines = append(fieldPipelines, fieldPipeline{
			field:    field,
			pipeline: s.pipelineForField(field),
		})
	}

	groups := make([]processedFieldGroup, 0, len(fieldPipelines))
	groupByFingerprint := make(map[string]int, len(fieldPipelines))
	for _, item := range fieldPipelines {
		descriptor := item.pipeline.Descriptor()
		if err := ValidatePipelineDescriptor(descriptor); err != nil {
			return nil, fmt.Errorf("fts: field %q: invalid configured pipeline descriptor: %w", item.field, err)
		}
		if indexed, ok := s.fieldPipelineDescriptor(item.field); ok && indexed.Fingerprint != descriptor.Fingerprint {
			return nil, fmt.Errorf("fts: field %q: configured pipeline fingerprint %q does not match indexed fingerprint %q", item.field, descriptor.Fingerprint, indexed.Fingerprint)
		}
		if index, ok := groupByFingerprint[descriptor.Fingerprint]; ok {
			groups[index].fields = append(groups[index].fields, item.field)
			continue
		}
		groupByFingerprint[descriptor.Fingerprint] = len(groups)
		groups = append(groups, processedFieldGroup{
			fields: []string{item.field},
			tokens: item.pipeline.Process(text),
		})
	}
	return groups, nil
}

func processedTokenCount(groups []processedFieldGroup) int {
	total := 0
	for _, group := range groups {
		total += len(group.tokens)
	}
	return total
}
