package main

import (
	"errors"
	"testing"
)

func TestRequireIsolatedAVIFDecoder(t *testing.T) {
	t.Run("system libavif loaded", func(t *testing.T) {
		if err := requireIsolatedAVIFDecoder(func() error { return nil }); err == nil {
			t.Fatal("err = nil, want an error refusing the dynamically loaded libavif")
		}
	})
	t.Run("dynamic loading disabled", func(t *testing.T) {
		if err := requireIsolatedAVIFDecoder(func() error { return errors.New("avif: dynamic disabled") }); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
	})
}
