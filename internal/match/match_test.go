package match

import "testing"

func TestMatch(t *testing.T) {
	m := New([]string{"hockey"}, []string{"en", "en-US", "en-CA"})
	en := []string{"en"}

	tests := []struct {
		name        string
		text        string
		langs       []string
		wantKeyword string
		wantKind    Kind
	}{
		// Whole-word matches.
		{"plain word", "going to the hockey game", en, "hockey", Word},
		{"capitalized", "Hockey night!", en, "hockey", Word},
		{"all caps", "HOCKEY", en, "hockey", Word},
		{"hashtag", "big win #hockey", en, "hockey", Word},
		{"punctuation", "hockey, hockey, hockey.", en, "hockey", Word},
		{"possessive", "hockey's best night", en, "hockey", Word},

		// Only inside a longer word.
		{"prefix", "watching icehockey", en, "hockey", Contains},
		{"suffix", "new hockeysticks", en, "hockey", Contains},
		{"camel-case hashtag", "#HockeyNightInCanada", en, "hockey", Contains},
		{"whole word wins over contains", "icehockey is still hockey", en, "hockey", Word},

		// No keyword.
		{"no keyword", "going to the football game", en, "", None},
		{"empty text", "", en, "", None},

		// Languages.
		{"en-US accepted", "hockey", []string{"en-US"}, "hockey", Word},
		{"en-CA accepted", "hockey", []string{"en-CA"}, "hockey", Word},
		{"tag case ignored", "hockey", []string{"EN-ca"}, "hockey", Word},
		{"one of several tags", "hockey", []string{"fr", "en"}, "hockey", Word},
		{"en-GB rejected", "hockey", []string{"en-GB"}, "", None},
		{"other language rejected", "hockey", []string{"fr"}, "", None},
		{"no language tag rejected", "hockey", nil, "", None},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKeyword, gotKind := m.Match(tt.text, tt.langs)
			if gotKeyword != tt.wantKeyword || gotKind != tt.wantKind {
				t.Errorf("Match(%q, %v) = (%q, %q), want (%q, %q)",
					tt.text, tt.langs, gotKeyword, gotKind, tt.wantKeyword, tt.wantKind)
			}
		})
	}
}

func TestMatchMultipleKeywords(t *testing.T) {
	m := New([]string{"hockey", " NHL "}, []string{"en"})

	keyword, kind := m.Match("NHL tonight", []string{"en"})
	if keyword != "nhl" || kind != Word {
		t.Errorf("got (%q, %q), want (\"nhl\", \"word\")", keyword, kind)
	}
}

func TestNoLanguagesAcceptsAll(t *testing.T) {
	m := New([]string{"hockey"}, nil)

	for _, langs := range [][]string{nil, {"fr"}, {"en-GB"}} {
		if _, kind := m.Match("hockey", langs); kind != Word {
			t.Errorf("langs %v: got %q, want \"word\"", langs, kind)
		}
	}
}
