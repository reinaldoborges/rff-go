package mysql

import (
	"testing"
)

func TestServerVersionRequirement(t *testing.T) {
	tests := []struct {
		version string
		wantErr bool
	}{
		{version: "8.0.36", wantErr: true},
		{version: "8.3.0", wantErr: true},
		{version: "8.4.0", wantErr: false},
		{version: "8.4.13-commercial", wantErr: false},
		{version: "9.0.0", wantErr: false},
		{version: "10.11.0-MariaDB", wantErr: true},
		{version: "invalid", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			gotErr := !supportedServerVersion(test.version)
			if gotErr != test.wantErr {
				t.Fatalf("version %q rejected = %t, want %t", test.version, gotErr, test.wantErr)
			}
		})
	}
}
