package main

import "errors"

// requireIsolatedAVIFDecoder refuses to start when gen2brain/avif loaded a
// system libavif: that decoder runs native code on untrusted remote media,
// where the WASM build keeps a decoder bug inside its sandbox. dynamic is
// avif.Dynamic, which returns nil only when the shared library loaded; a
// build with -tags nodynamic never tries to load it.
func requireIsolatedAVIFDecoder(dynamic func() error) error {
	if dynamic() == nil {
		return errors.New("avif: a system libavif was loaded; build with -tags nodynamic so AVIF decoding stays in the WASM sandbox")
	}
	return nil
}
