package fts

import (
	"fmt"
	"strings"
	"unicode"
)

func ParseQuery(input string) (Query, error) {
	p := &queryParser{input: input}
	clauses, err := p.parse()
	if err != nil {
		return nil, err
	}
	if len(clauses) == 0 {
		return &BooleanQuery{}, nil
	}
	if len(clauses) == 1 && clauses[0].Occur == Should {
		return clauses[0].Query, nil
	}
	return &BooleanQuery{Clauses: clauses}, nil
}

type queryParser struct {
	input string
	pos   int
}

func (p *queryParser) parse() ([]BoolClause, error) {
	var clauses []BoolClause
	for {
		p.skipSpaces()
		if p.pos >= len(p.input) {
			return clauses, nil
		}

		occur := Should
		switch p.input[p.pos] {
		case '+':
			occur = Must
			p.pos++
		case '-':
			occur = MustNot
			p.pos++
		}

		field, err := p.maybeField()
		if err != nil {
			return nil, err
		}

		if p.pos >= len(p.input) {
			return nil, fmt.Errorf("fts: parse query: unexpected end after prefix/field at %d", p.pos)
		}

		var q Query
		if p.input[p.pos] == '"' {
			phrase, err := p.readQuoted()
			if err != nil {
				return nil, err
			}
			q = PhraseQuery{Field: field, Phrase: phrase}
		} else {
			word := p.readWord()
			if word == "" {
				return nil, fmt.Errorf("fts: parse query: empty term at %d", p.pos)
			}
			if strings.HasSuffix(word, "*") && len(word) > 1 {
				q = PrefixQuery{Field: field, Prefix: strings.TrimSuffix(word, "*")}
			} else {
				q = TermQuery{Field: field, Term: word}
			}
		}

		clauses = append(clauses, BoolClause{Occur: occur, Query: q})
	}
}

func (p *queryParser) maybeField() (string, error) {
	save := p.pos
	var b strings.Builder
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ':' {
			name := b.String()
			if name == "" {
				p.pos = save
				return "", nil
			}
			p.pos++
			return name, nil
		}
		if unicode.IsSpace(rune(c)) || c == '"' || c == '+' || c == '-' {
			p.pos = save
			return "", nil
		}
		b.WriteByte(c)
		p.pos++
	}
	p.pos = save
	return "", nil
}

func (p *queryParser) readQuoted() (string, error) {
	p.pos++
	start := p.pos
	for p.pos < len(p.input) && p.input[p.pos] != '"' {
		p.pos++
	}
	if p.pos >= len(p.input) {
		return "", fmt.Errorf("fts: parse query: unterminated quote starting at %d", start-1)
	}
	phrase := p.input[start:p.pos]
	p.pos++
	return phrase, nil
}

func (p *queryParser) readWord() string {
	start := p.pos
	for p.pos < len(p.input) && !unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
	return p.input[start:p.pos]
}

func (p *queryParser) skipSpaces() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}
