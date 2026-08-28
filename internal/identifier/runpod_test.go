// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package identifier

import (
	"strings"
	"testing"
)

func TestValidateRunPodID(t *testing.T) {
	for _, valid := range []string{"abc123", "template_1", "endpoint-1"} {
		if err := ValidateRunPodID(valid); err != nil {
			t.Fatalf("valid ID %q rejected: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", ".", "..", "../pods/x", "a/b", "a%2Fb", "a?x", "a#x", "a\nb", strings.Repeat("a", 192)} {
		if err := ValidateRunPodID(invalid); err == nil {
			t.Fatalf("unsafe ID %q accepted", invalid)
		}
	}
}
