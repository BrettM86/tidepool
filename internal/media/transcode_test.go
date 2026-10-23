package media

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"testing"
	"time"

	"github.com/gen2brain/avif"
)

const generousMaxBytes = 10 << 20

// boundedContext keeps a leaked decode slot from hanging the run.
func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestTranscodeAVIFOpaqueBecomesJPEG(t *testing.T) {
	source := encodeAVIF(t, uniformImage(64, 48, color.NRGBA{R: 200, G: 100, B: 50, A: 255}), losslessOptions)

	out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
	}
	if !bytes.HasPrefix(out, []byte{0xff, 0xd8}) {
		t.Fatalf("output does not start with the JPEG SOI marker FF D8")
	}
	img := decodeJPEGOutput(t, out)
	assertDimensions(t, img, 64, 48)
	assertColourNear(t, "centre pixel", nrgbaAt(img, 32, 24), color.NRGBA{R: 200, G: 100, B: 50, A: 255}, 6)
}

func TestTranscodeAVIFTranslucentBecomesPNG(t *testing.T) {
	opaque := color.NRGBA{R: 30, G: 160, B: 90, A: 255}
	halfTransparent := color.NRGBA{R: 200, G: 40, B: 40, A: 128}

	fullyTransparentLeftHalf := uniformImage(64, 48, opaque)
	halfTransparentLeftHalf := uniformImage(64, 48, opaque)
	for y := 0; y < 48; y++ {
		for x := 0; x < 32; x++ {
			fullyTransparentLeftHalf.SetNRGBA(x, y, color.NRGBA{})
			halfTransparentLeftHalf.SetNRGBA(x, y, halfTransparent)
		}
	}

	tests := []struct {
		name               string
		source             *image.NRGBA
		wantLeftAlpha      int
		leftAlphaTolerance int
		wantLeftColour     *color.NRGBA
	}{
		{name: "fully transparent region", source: fullyTransparentLeftHalf, wantLeftAlpha: 0, leftAlphaTolerance: 2},
		{name: "alpha 128 region and no alpha 0 pixel", source: halfTransparentLeftHalf, wantLeftAlpha: 128, leftAlphaTolerance: 3, wantLeftColour: &halfTransparent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := encodeAVIF(t, test.source, losslessOptions)

			out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
			if err != nil {
				t.Fatalf("TranscodeAVIF: %v", err)
			}
			if mimeType != "image/png" {
				t.Fatalf("mimeType = %q, want image/png", mimeType)
			}
			img := decodePNGOutput(t, out)
			assertDimensions(t, img, 64, 48)

			left := nrgbaAt(img, 8, 24)
			if difference := int(left.A) - test.wantLeftAlpha; difference < -test.leftAlphaTolerance || difference > test.leftAlphaTolerance {
				t.Errorf("left-half alpha = %d, want %d ±%d", left.A, test.wantLeftAlpha, test.leftAlphaTolerance)
			}
			if test.wantLeftColour != nil {
				assertColourNear(t, "left-half colour", left, *test.wantLeftColour, 6)
			}

			right := nrgbaAt(img, 56, 24)
			if right.A != 255 {
				t.Errorf("right-half alpha = %d, want 255", right.A)
			}
			assertColourNear(t, "right-half colour", right, opaque, 3)
		})
	}
}

func TestTranscodeAVIFCapsLongEdge(t *testing.T) {
	tests := []struct {
		name                  string
		width, height         int
		wantWidth, wantHeight int
	}{
		{name: "wide 5000x100", width: 5000, height: 100, wantWidth: 4096, wantHeight: 82},
		{name: "tall 100x5000", width: 100, height: 5000, wantWidth: 82, wantHeight: 4096},
		{name: "already at cap 4096x10", width: 4096, height: 10, wantWidth: 4096, wantHeight: 10},
		{name: "small 30x20 is not upscaled", width: 30, height: 20, wantWidth: 30, wantHeight: 20},
		{name: "half rounds up 8192x5", width: 8192, height: 5, wantWidth: 4096, wantHeight: 3},
		{name: "short side never below 1 8192x1", width: 8192, height: 1, wantWidth: 4096, wantHeight: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := encodeAVIF(t, uniformImage(test.width, test.height, color.NRGBA{R: 90, G: 120, B: 200, A: 255}), fastOptions)

			out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
			if err != nil {
				t.Fatalf("TranscodeAVIF: %v", err)
			}
			if mimeType != "image/jpeg" {
				t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
			}
			assertDimensions(t, decodeJPEGOutput(t, out), test.wantWidth, test.wantHeight)
		})
	}
}

func TestTranscodeAVIFAnimatedUsesFirstFrame(t *testing.T) {
	red := uniformImage(32, 32, color.NRGBA{R: 255, A: 255})
	blue := uniformImage(32, 32, color.NRGBA{B: 255, A: 255})
	source := encodeAnimatedAVIF(t, []image.Image{red, blue}, losslessOptions)

	out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
	}
	img := decodeJPEGOutput(t, out)
	assertDimensions(t, img, 32, 32)
	assertColourNear(t, "centre pixel", nrgbaAt(img, 16, 16), color.NRGBA{R: 255, A: 255}, 8)
}

func TestTranscodeAVIFMalformedInputIsUndecodable(t *testing.T) {
	valid := encodeAVIF(t, uniformImage(16, 16, color.NRGBA{R: 10, G: 200, B: 10, A: 255}), losslessOptions)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty bytes", data: []byte{}},
		{name: "PNG bytes", data: encodePNGFixture(t, uniformImage(8, 8, color.NRGBA{R: 1, G: 2, B: 3, A: 255}))},
		{name: "truncated AVIF", data: valid[:len(valid)/2]},
		{name: "empty pitm box", data: withEmptyPitm(t, valid)},
		{name: "64-bit box size 0x7ffffffffffffff0", data: withOverflowingLargeSize(t, valid)},
		{name: "unparseable item locations", data: withUnparseableItemLocations(t, valid)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("panic escaped TranscodeAVIF: %v", recovered)
				}
			}()
			_, _, err := TranscodeAVIF(boundedContext(t), test.data, generousMaxBytes)
			if !errors.Is(err, ErrUndecodable) {
				t.Fatalf("err = %v, want ErrUndecodable", err)
			}
		})
	}

	t.Run("valid transcode afterwards still gets a decode slot", func(t *testing.T) {
		_, mimeType, err := TranscodeAVIF(boundedContext(t), valid, generousMaxBytes)
		if err != nil {
			t.Fatalf("TranscodeAVIF: %v", err)
		}
		if mimeType != "image/jpeg" {
			t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
		}
	})
}

func TestTranscodeAVIFDeclaredDimensionsOverCap(t *testing.T) {
	small := encodeAVIF(t, uniformImage(16, 16, color.NRGBA{R: 70, G: 70, B: 70, A: 255}), losslessOptions)

	tests := []struct {
		name string
		data []byte
	}{
		{name: "wide side of 8193", data: encodeAVIF(t, uniformImage(8193, 8, color.NRGBA{R: 70, G: 70, B: 70, A: 255}), fastOptions)},
		{name: "tall side of 8193", data: encodeAVIF(t, uniformImage(16, 8193, color.NRGBA{R: 70, G: 70, B: 70, A: 255}), fastOptions)},
		{name: "declared 8000x6000 over the pixel budget", data: withDeclaredDimensions(t, small, 8000, 6000)},
		// Full decode of this file fails, so only a header check before decoding yields ErrImageTooLarge.
		{name: "declared 8000x6000 with unparseable item locations", data: withUnparseableItemLocations(t, withDeclaredDimensions(t, small, 8000, 6000))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := TranscodeAVIF(boundedContext(t), test.data, generousMaxBytes)
			if !errors.Is(err, ErrImageTooLarge) {
				t.Fatalf("err = %v, want ErrImageTooLarge", err)
			}
		})
	}
}

func TestTranscodeAVIFEncodedOutputOverLimit(t *testing.T) {
	source := encodeAVIF(t, noiseImage(64, 48, 7), losslessOptions)

	_, _, err := TranscodeAVIF(boundedContext(t), source, 64)
	if !errors.Is(err, ErrEncodedTooLarge) {
		t.Fatalf("err = %v, want ErrEncodedTooLarge", err)
	}
}

func TestTranscodeAVIFWaitsForDecodeSlot(t *testing.T) {
	source := encodeAVIF(t, uniformImage(16, 16, color.NRGBA{R: 120, G: 30, B: 220, A: 255}), losslessOptions)

	decodeSlots <- struct{}{}
	slotHeld := true
	t.Cleanup(func() {
		if slotHeld {
			<-decodeSlots
		}
	})

	shortContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := TranscodeAVIF(shortContext, source, generousMaxBytes); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with every decode slot taken: err = %v, want context.DeadlineExceeded", err)
	}

	<-decodeSlots
	slotHeld = false

	_, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("after the slot is free: TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("after the slot is free: mimeType = %q, want image/jpeg", mimeType)
	}
}

// A caller whose context has a long deadline still gives up after
// decodeSlotWait, so a stuck decode cannot queue every ingest behind it.
func TestTranscodeAVIFGivesUpWaitingForDecodeSlot(t *testing.T) {
	source := encodeAVIF(t, uniformImage(16, 16, color.NRGBA{R: 120, G: 30, B: 220, A: 255}), losslessOptions)

	originalWait := decodeSlotWait
	decodeSlotWait = 50 * time.Millisecond
	t.Cleanup(func() { decodeSlotWait = originalWait })

	decodeSlots <- struct{}{}
	slotHeld := true
	t.Cleanup(func() {
		if slotHeld {
			<-decodeSlots
		}
	})

	_, _, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if !errors.Is(err, ErrDecoderBusy) {
		t.Fatalf("with every decode slot taken: err = %v, want ErrDecoderBusy", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with every decode slot taken: err = %v, want the slot wait to end before the context deadline", err)
	}

	<-decodeSlots
	slotHeld = false

	_, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("after the slot is free: TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("after the slot is free: mimeType = %q, want image/jpeg", mimeType)
	}
}

func TestCheckDecoder(t *testing.T) {
	if err := CheckDecoder(boundedContext(t)); err != nil {
		t.Fatalf("CheckDecoder: %v, want nil", err)
	}
}

// A context cancelled before the call fails it even when a slot is free,
// so a cancelled ingest never starts an uncancellable decode. A select over a
// free slot and a done context picks either case at random, hence the repeats.
func TestTranscodeAVIFCancelledContextWithFreeSlot(t *testing.T) {
	source := encodeAVIF(t, uniformImage(16, 16, color.NRGBA{R: 120, G: 30, B: 220, A: 255}), losslessOptions)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for attempt := range 50 {
		if _, _, err := TranscodeAVIF(cancelled, source, generousMaxBytes); !errors.Is(err, context.Canceled) {
			t.Fatalf("attempt %d with a cancelled context and a free slot: err = %v, want context.Canceled", attempt, err)
		}
	}

	_, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("after the cancelled calls: TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("after the cancelled calls: mimeType = %q, want image/jpeg", mimeType)
	}
}

// A high-bit-depth source (decoded as *image.RGBA64) still becomes an
// 8-bit PNG; a 16-bit PNG doubles the stored size for no visible gain.
func TestTranscodeAVIFHighBitDepthBecomesEightBitPNG(t *testing.T) {
	opaque := color.NRGBA{R: 30, G: 160, B: 90, A: 255}
	halfTransparent := color.NRGBA{R: 200, G: 40, B: 40, A: 128}
	halfTransparentLeftHalf := uniformImage(64, 48, opaque)
	for y := 0; y < 48; y++ {
		for x := 0; x < 32; x++ {
			halfTransparentLeftHalf.SetNRGBA(x, y, halfTransparent)
		}
	}
	source := withTenBitCodecConfig(t, encodeAVIF(t, halfTransparentLeftHalf, losslessOptions))
	if decoded, err := avif.Decode(bytes.NewReader(source)); err != nil {
		t.Fatalf("fixture: avif.Decode: %v", err)
	} else if _, ok := decoded.(*image.RGBA64); !ok {
		t.Fatalf("fixture: avif.Decode returned %T, want *image.RGBA64", decoded)
	}

	out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("TranscodeAVIF: %v", err)
	}
	if mimeType != "image/png" {
		t.Fatalf("mimeType = %q, want image/png", mimeType)
	}
	config, err := png.DecodeConfig(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("png.DecodeConfig(output): %v", err)
	}
	switch config.ColorModel {
	case color.NRGBAModel, color.RGBAModel:
	case color.NRGBA64Model, color.RGBA64Model:
		t.Fatalf("output PNG is 16 bits per channel, want 8")
	default:
		t.Fatalf("output PNG colour model %v, want NRGBAModel or RGBAModel", config.ColorModel)
	}
	img := decodePNGOutput(t, out)
	assertDimensions(t, img, 64, 48)
	left := nrgbaAt(img, 8, 24)
	if difference := int(left.A) - 128; difference < -3 || difference > 3 {
		t.Errorf("left-half alpha = %d, want 128 ±3", left.A)
	}
	assertColourNear(t, "left-half colour", left, halfTransparent, 6)
	right := nrgbaAt(img, 56, 24)
	if right.A != 255 {
		t.Errorf("right-half alpha = %d, want 255", right.A)
	}
	assertColourNear(t, "right-half colour", right, opaque, 3)
}

// An opaque high-bit-depth source (decoded as *image.RGBA64) still becomes a
// JPEG: opacity is read from the 16-bit image, not only from *image.RGBA.
func TestTranscodeAVIFOpaqueHighBitDepthBecomesJPEG(t *testing.T) {
	source := withTenBitCodecConfig(t, encodeAVIF(t, uniformImage(64, 48, color.NRGBA{R: 200, G: 100, B: 50, A: 255}), losslessOptions))
	if decoded, err := avif.Decode(bytes.NewReader(source)); err != nil {
		t.Fatalf("fixture: avif.Decode: %v", err)
	} else if _, ok := decoded.(*image.RGBA64); !ok {
		t.Fatalf("fixture: avif.Decode returned %T, want *image.RGBA64", decoded)
	}

	out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
	}
	img := decodeJPEGOutput(t, out)
	assertDimensions(t, img, 64, 48)
	assertColourNear(t, "centre pixel", nrgbaAt(img, 32, 24), color.NRGBA{R: 200, G: 100, B: 50, A: 255}, 6)
}

func TestIsAVIF(t *testing.T) {
	still := encodeAVIF(t, uniformImage(8, 8, color.NRGBA{R: 40, G: 80, B: 120, A: 255}), losslessOptions)
	animated := encodeAnimatedAVIF(t, []image.Image{
		uniformImage(8, 8, color.NRGBA{R: 255, A: 255}),
		uniformImage(8, 8, color.NRGBA{B: 255, A: 255}),
	}, losslessOptions)

	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{name: "generated still AVIF", data: still, want: true},
		{name: "generated animated AVIF", data: animated, want: true},
		{name: "ftyp major brand avif", data: []byte{0, 0, 0, 0x14, 'f', 't', 'y', 'p', 'a', 'v', 'i', 'f', 0, 0, 0, 0, 'm', 'i', 'f', '1'}, want: true},
		{name: "ftyp major brand avis", data: []byte{0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'a', 'v', 'i', 's', 0, 0, 0, 0, 'a', 'v', 'i', 's', 'm', 's', 'f', '1'}, want: true},
		{name: "ftyp major brand mif1 with compatible brand avif", data: []byte{0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'm', 'i', 'f', '1', 0, 0, 0, 0, 'm', 'i', 'f', '1', 'a', 'v', 'i', 'f'}, want: true},
		{name: "ftyp major brand mif1 with compatible brands miaf and avis", data: []byte{0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'm', 'i', 'f', '1', 0, 0, 0, 0, 'm', 'i', 'a', 'f', 'a', 'v', 'i', 's'}, want: true},
		// The ftyp box ends after its minor version; the avif that follows belongs to the next box.
		{name: "ftyp declared size ends before the compatible brands", data: []byte{0, 0, 0, 0x10, 'f', 't', 'y', 'p', 'm', 'i', 'f', '1', 0, 0, 0, 0, 'a', 'v', 'i', 'f'}, want: false},
		{name: "ftyp declared size below the 16-byte minimum", data: []byte{0, 0, 0, 0x0c, 'f', 't', 'y', 'p', 'm', 'i', 'f', '1', 0, 0, 0, 0, 'a', 'v', 'i', 'f'}, want: false},
		// Declares 32 bytes, so a compatible avif would sit at bytes 24-27; the data stops inside it.
		{name: "ftyp declared size beyond the data", data: []byte{0, 0, 0, 0x20, 'f', 't', 'y', 'p', 'm', 'i', 'f', '1', 0, 0, 0, 0, 'm', 'i', 'f', '1', 'm', 'i', 'a', 'f', 'a', 'v'}, want: false},
		{name: "ftyp major brand heic", data: []byte{0, 0, 0, 0x18, 'f', 't', 'y', 'p', 'h', 'e', 'i', 'c', 0, 0, 0, 0, 'm', 'i', 'f', '1', 'h', 'e', 'i', 'c'}, want: false},
		{name: "PNG", data: encodePNGFixture(t, uniformImage(8, 8, color.NRGBA{R: 1, G: 2, B: 3, A: 255})), want: false},
		{name: "JPEG", data: encodeJPEGFixture(t, uniformImage(8, 8, color.NRGBA{R: 1, G: 2, B: 3, A: 255})), want: false},
		{name: "empty", data: []byte{}, want: false},
		{name: "short input cut inside the major brand", data: []byte{0, 0, 0, 0x14, 'f', 't', 'y', 'p', 'a', 'v', 'i'}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsAVIF(test.data); got != test.want {
				t.Fatalf("IsAVIF = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTranscodeAVIFAppliesRotation(t *testing.T) {
	red := color.NRGBA{R: 255, A: 255}
	blue := color.NRGBA{B: 255, A: 255}
	leftRedRightBlue := uniformImage(40, 20, blue)
	for y := 0; y < 20; y++ {
		for x := 0; x < 20; x++ {
			leftRedRightBlue.SetNRGBA(x, y, red)
		}
	}
	// irot angle 1 is 90° anti-clockwise: the left edge becomes the bottom edge.
	source := withRotation(t, encodeAVIF(t, leftRedRightBlue, losslessOptions), 1)

	out, mimeType, err := TranscodeAVIF(boundedContext(t), source, generousMaxBytes)
	if err != nil {
		t.Fatalf("TranscodeAVIF: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
	}
	img := decodeJPEGOutput(t, out)
	assertDimensions(t, img, 20, 40)
	assertColourNear(t, "top-half pixel", nrgbaAt(img, 10, 10), blue, 8)
	assertColourNear(t, "bottom-half pixel", nrgbaAt(img, 10, 30), red, 8)
}
