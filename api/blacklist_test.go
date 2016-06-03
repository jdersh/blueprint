package api

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestBlacklist(t *testing.T) {
	var err error
	s := &server{"", nil, nil, "this_file_should_not_exist.json"}
	_, err = s.isBlacklisted("hello world")
	if err == nil {
		t.Errorf("The file %v should not exist and should return an error!", s.blacklistFilename)
	}

	var jsonFile *os.File
	jsonFile, err = ioutil.TempFile("./", "testJson")
	if err != nil {
		t.Errorf("%v", err)
	}
	defer os.Remove(jsonFile.Name())

	jsonFile.WriteString(`["^wow$", "^logs\\.dfp_.*$", "^logs\\.a.c_.*$"]`)
	s.blacklistFilename = jsonFile.Name()
	err = jsonFile.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	var tests = []struct {
		input string
		want  bool
	}{
		{"", false},
		{"wow", true},
		{"wow_", false},

		{"logsadfp", false},
		{"logsadfp_", false},
		{"logsadfp_a", false},
		{"logsadfp_abc", false},

		{"logs.abc_", true},
		{"logs.aec", false},
		{"logs.ac", false},
		{"logs.a.c_", true},
		{"logs.a.c_wow", true},

		{"logs.dfp", false},
		{"logs.dfp_", true},
		{"logs.dfp_a", true},
		{"logs.dfp_abc", true},

		{"lOgs.dfp", false},
		{"Logs.dfp_", true},
		{"lOgs.dfp_a", true},
		{"loGs.dfp_abc", true},
	}

	for _, test := range tests {
		if got, err := s.isBlacklisted(test.input); got != test.want || err != nil {
			t.Errorf("blacklist(%v) = %v, err = %v", test.input, got, err)
		}
	}

}
