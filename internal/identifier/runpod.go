// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package identifier contains the fixed-host RunPod identifier boundary shared
// by the management client, inference verifier, and model-router.
package identifier

import (
	"errors"
	"regexp"
)

var runPodID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const maxRunPodIDBytes = 191

// ValidateRunPodID rejects every character that could affect a URL path. It is
// intentionally stricter than URL escaping because gateways may normalize
// encoded slashes and dot segments before routing.
func ValidateRunPodID(value string) error {
	if len(value) == 0 || len(value) > maxRunPodIDBytes || !runPodID.MatchString(value) {
		return errors.New("RunPod resource ID is empty or malformed")
	}
	return nil
}
