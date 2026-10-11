// Package match decides whether a post is of interest: it is in one of the
// configured languages and mentions one of the configured keywords. It is
// pure logic with no I/O, so it is easy to test.
package match

import (
	"strings"
	"unicode"
)

// Kind says how a keyword matched.
type Kind string

const (
	None     Kind = ""         // no keyword matched
	Word     Kind = "word"     // the keyword appears as a whole word: "#hockey", "hockey!"
	Contains Kind = "contains" // the keyword only appears inside a longer word: "icehockey"
)

// Matcher holds the keywords and languages to look for. Build one with New.
type Matcher struct {
	keywords  []string        // lowercased, in the order given
	isKeyword map[string]bool // the same keywords, for fast whole-word lookups
	langs     map[string]bool // lowercased language tags
}

// New returns a Matcher. Matching ignores case. Keywords are single words.
// Language tags must match exactly (so "en" does not accept "en-GB"); an
// empty langs list accepts posts in any language.
func New(keywords, langs []string) *Matcher {
	m := &Matcher{
		isKeyword: map[string]bool{},
		langs:     map[string]bool{},
	}
	for _, k := range keywords {
		k = strings.ToLower(strings.TrimSpace(k))
		if k != "" {
			m.keywords = append(m.keywords, k)
			m.isKeyword[k] = true
		}
	}
	for _, l := range langs {
		l = strings.ToLower(strings.TrimSpace(l))
		if l != "" {
			m.langs[l] = true
		}
	}
	return m
}

// Match reports which keyword a post matches and how. A whole-word match on
// any keyword wins over a contains match. It returns ("", None) if the post
// isn't in one of the languages or mentions no keyword.
func (m *Matcher) Match(text string, postLangs []string) (keyword string, kind Kind) {
	if !m.langOK(postLangs) {
		return "", None
	}

	// Cheap check first: most posts contain no keyword at all, and a keyword
	// can't be a whole word without also being a substring.
	lower := strings.ToLower(text)
	contained := ""
	for _, k := range m.keywords {
		if strings.Contains(lower, k) {
			contained = k
			break
		}
	}
	if contained == "" {
		return "", None
	}

	// Split into words on anything that isn't a letter or digit, so "#hockey",
	// "hockey!" and "hockey's" all yield the word "hockey".
	words := strings.FieldsFunc(lower, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	for _, w := range words {
		if m.isKeyword[w] {
			return w, Word
		}
	}
	return contained, Contains
}

// langOK reports whether any of the post's language tags is configured.
func (m *Matcher) langOK(postLangs []string) bool {
	if len(m.langs) == 0 {
		return true
	}
	for _, l := range postLangs {
		if m.langs[strings.ToLower(l)] {
			return true
		}
	}
	return false
}
