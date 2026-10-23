// Package media converts remote images into formats the Coves lexicons
// accept. Coves blob slots take png, jpeg, webp and gif only, while some
// Lemmy instances (lemmy.zip among them) serve their pictures as AVIF, so a
// bridged post would otherwise lose its images.
//
// AVIF is decoded by gen2brain/avif, which runs libavif compiled to WASM, so
// a bug in the decoder stays inside the WASM sandbox instead of the process.
// The binary must be built with -tags nodynamic: without it the package
// quietly prefers a system libavif loaded through purego, which runs native
// code on untrusted bytes. Only one decode runs at a time, because a decode
// cannot be cancelled once started and can briefly use gigabytes of memory.
//
// A transcode waits at most decodeSlotWait for that slot and then gives up
// with ErrDecoderBusy. Transcodes run inside the ingest workers (four, each
// on a two-minute lease); a hostile instance posting many large AVIFs must
// not leave every worker queued on the one slot until its lease expires,
// which would retry and eventually poison unrelated events.
//
// Known limits:
//   - The dimension caps check the primary item's declared size (ispe) as
//     gen2brain/avif's Go ISOBMFF parser reports it; an image sequence's
//     track dimensions are not checked. A file that declares a small size
//     but carries a larger frame still decodes at full size; bounding that
//     needs the AV1 sequence header parsed.
//   - gen2brain/avif v0.6.0 ignores a failed frame decode, so a corrupt AV1
//     payload behind valid headers yields uninitialised output with no error.
//   - No colour management: ICC profiles and wide-gamut primaries are not
//     converted to sRGB.
//   - The transcoded image must fit the slot's byte cap. An AVIF that
//     re-encodes larger (a big translucent PNG, or a 4096px avatar over
//     1 MB) is omitted, not retried at a smaller size.
//   - The WASM module's stdout and stderr are the process's, so dav1d and
//     libavif diagnostics may appear in the logs unstructured.
package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"runtime/debug"
	"time"

	"github.com/gen2brain/avif"
	"golang.org/x/image/draw"
)

const (
	// maxDeclaredSide and maxDeclaredPixels bound the size a file declares,
	// not what its decode allocates (see Known limits). At 40 million pixels
	// one RGBA frame is 160 MB at 8 bits, or 320 MB at high bit depth.
	// libavif holds it twice in WASM linear memory (its RGB scratch and the
	// output buffer), and linear memory never shrinks and grows by copying.
	// High-depth output is copied once more onto the Go heap and again for
	// the 8-bit conversion, AutoRotate adds a full copy, and CatmullRom
	// scaling adds a float64 temporary buffer. A 40-megapixel decode was
	// measured allocating about 1.9 GB.
	maxDeclaredSide   = 8192
	maxDeclaredPixels = 40_000_000
	// releaseMemoryAbovePixels: after a transcode whose declared size exceeds
	// this many pixels, the dead decode buffers are returned to the OS before
	// the slot is released. wazero grows linear memory by copying, so old and
	// new buffers briefly coexist, and Go's GC pacing would otherwise let a
	// second large decode stack on the first's garbage: two 40-megapixel
	// decodes back to back reached 6.8 GB of heap in an 8 GiB container.
	releaseMemoryAbovePixels = 4_000_000
	// maxOutputLongEdge caps the stored image's long edge.
	maxOutputLongEdge = 4096
	jpegQuality       = 90
)

var (
	// ErrImageTooLarge: the declared dimensions exceed the decode caps, so
	// the image was rejected before any pixel was decoded.
	ErrImageTooLarge = errors.New("media: image too large")
	// ErrUndecodable: the bytes are not a decodable AVIF, including a
	// decoder panic on crafted input.
	ErrUndecodable = errors.New("media: undecodable image")
	// ErrEncodedTooLarge: the transcoded image exceeds the caller's byte cap.
	ErrEncodedTooLarge = errors.New("media: encoded image too large")
	// ErrDecoderBusy: no decode slot came free within decodeSlotWait.
	ErrDecoderBusy = errors.New("media: decoder busy")

	// decodeSlots holds one token per decode allowed to run at once.
	decodeSlots = make(chan struct{}, 1)
	// decodeSlotWait bounds how long a transcode waits for a decode slot,
	// whatever the caller's deadline, so that slot contention costs an
	// ingest worker seconds rather than its whole lease (see the package
	// doc).
	decodeSlotWait = 5 * time.Second
)

// CheckDecoder transcodes a tiny generated AVIF and returns an error if that
// fails. Run it at startup: gen2brain/avif compiles its WASM module lazily
// on first use, inside a sync.OnceFunc that re-panics on every later call if
// compilation failed, which would make every AVIF silently undecodable. The
// check also moves the roughly half-second compile out of the first ingest.
func CheckDecoder(ctx context.Context) (err error) {
	// Encoding runs the same lazy compile, which panics when it fails.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("media: decoder check: encoder panic: %v", recovered)
		}
	}()
	sample := image.NewNRGBA(image.Rect(0, 0, 8, 8))
	for index := range sample.Pix {
		sample.Pix[index] = 0xff
	}
	sample.Set(0, 0, color.NRGBA{R: 200, G: 100, B: 50, A: 255})
	var encoded bytes.Buffer
	options := avif.Options{Quality: 60, QualityAlpha: 60, Speed: 10, ChromaSubsampling: image.YCbCrSubsampleRatio420}
	if err = avif.Encode(&encoded, sample, options); err != nil {
		return fmt.Errorf("media: decoder check: encoding sample AVIF: %w", err)
	}
	if _, _, err = TranscodeAVIF(ctx, encoded.Bytes(), 1_000_000); err != nil {
		return fmt.Errorf("media: decoder check: %w", err)
	}
	return nil
}

// TranscodeAVIF decodes the first frame of an AVIF (applying its rotation and
// mirroring), scales it so the long edge is at most 4096 pixels, and encodes
// it as PNG when any pixel is not fully opaque, otherwise as JPEG. It returns
// the encoded bytes and their mime type. maxBytes caps the encoded output.
//
// It first waits for the single process-wide decode slot, giving up after
// decodeSlotWait with ErrDecoderBusy, or earlier with ctx's error. Other
// errors wrap ErrImageTooLarge, ErrUndecodable or ErrEncodedTooLarge.
func TranscodeAVIF(ctx context.Context, data []byte, maxBytes int64) ([]byte, string, error) {
	if err := acquireDecodeSlot(ctx); err != nil {
		return nil, "", err
	}
	defer func() { <-decodeSlots }()
	// select picks at random when the slot is free and ctx is already done,
	// so check again: a cancelled caller must never start a decode.
	if err := ctx.Err(); err != nil {
		return nil, "", fmt.Errorf("media: cancelled before decoding: %w", err)
	}

	config, err := decodeConfig(data)
	if err != nil {
		return nil, "", err
	}
	if config.Width <= 0 || config.Height <= 0 {
		return nil, "", fmt.Errorf("%w: declared dimensions %dx%d", ErrUndecodable, config.Width, config.Height)
	}
	declaredPixels := int64(config.Width) * int64(config.Height)
	if config.Width > maxDeclaredSide || config.Height > maxDeclaredSide || declaredPixels > maxDeclaredPixels {
		return nil, "", fmt.Errorf("%w: declared dimensions %dx%d", ErrImageTooLarge, config.Width, config.Height)
	}

	out, mimeType, err := decodeAndEncode(data)
	if declaredPixels > releaseMemoryAbovePixels {
		debug.FreeOSMemory()
	}
	if err != nil {
		return nil, "", err
	}
	if int64(len(out)) > maxBytes {
		return nil, "", fmt.Errorf("%w: %d bytes as %s, limit %d", ErrEncodedTooLarge, len(out), mimeType, maxBytes)
	}
	return out, mimeType, nil
}

// acquireDecodeSlot takes a decode slot, giving up after decodeSlotWait with
// ErrDecoderBusy or when ctx is done with ctx's error.
func acquireDecodeSlot(ctx context.Context) error {
	timer := time.NewTimer(decodeSlotWait)
	defer timer.Stop()
	select {
	case decodeSlots <- struct{}{}:
		return nil
	case <-timer.C:
		return fmt.Errorf("%w: no decode slot free after %s", ErrDecoderBusy, decodeSlotWait)
	case <-ctx.Done():
		return fmt.Errorf("media: waiting for a decode slot: %w", ctx.Err())
	}
}

// decodeAndEncode decodes the first frame and encodes it for storage. The
// decoded image lives only inside this call, so once it returns the decode
// buffers are garbage the caller can hand back to the OS.
func decodeAndEncode(data []byte) ([]byte, string, error) {
	decoded, err := decodeFirstFrame(data)
	if err != nil {
		return nil, "", err
	}
	// For 8-bit output with no orientation change the decoded pixels alias
	// the decoder's WASM linear memory, so holding them keeps all of it
	// alive; here they are scaled, encoded and dropped. Opacity is read
	// before scaling and 8-bit conversion so filter rounding cannot change
	// the output format.
	return encode(toEightBit(decoded), isOpaque(decoded))
}

// decodeConfig reads the declared dimensions, turning a panic in
// gen2brain/avif v0.6.0's Go ISOBMFF pre-parser (parseAVIFProps in
// isobmff.go, which panics on some crafted box layouts) into ErrUndecodable.
func decodeConfig(data []byte) (config image.Config, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: decoder panic reading dimensions: %v", ErrUndecodable, recovered)
		}
	}()
	config, err = avif.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return image.Config{}, fmt.Errorf("%w: reading dimensions: %w", ErrUndecodable, err)
	}
	return config, nil
}

// decodeFirstFrame decodes only the first frame of a still or animated AVIF.
// The generic image.Decode is never used: avif registers itself with the
// image package, and untrusted bytes must not pick their own decoder. A
// panic, from gen2brain/avif's Go ISOBMFF pre-parser (parseAVIFProps in
// isobmff.go, run for AutoRotate) or from a failed lazy WASM compile, becomes
// ErrUndecodable.
func decodeFirstFrame(data []byte) (img image.Image, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: decoder panic: %v", ErrUndecodable, recovered)
		}
	}()
	img, err = avif.Decode(bytes.NewReader(data), avif.Options{AutoRotate: true})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUndecodable, err)
	}
	return img, nil
}

// toEightBit returns img as 8-bit RGBA with its long edge capped at
// maxOutputLongEdge; the short side is rounded half up and never below 1.
// Smaller images are never upscaled, and an *image.RGBA that needs no
// scaling is returned unchanged. Anything else (the decoder's *image.RGBA64
// for high bit depth) is converted, so the PNG encoder never writes 16 bits
// per channel.
func toEightBit(img image.Image) *image.RGBA {
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	longEdge := max(width, height)
	if longEdge <= maxOutputLongEdge {
		if rgba, ok := img.(*image.RGBA); ok {
			return rgba
		}
		converted := image.NewRGBA(image.Rect(0, 0, width, height))
		draw.Draw(converted, converted.Bounds(), img, bounds.Min, draw.Src)
		return converted
	}
	shortSide := func(side int) int {
		return max(1, (2*side*maxOutputLongEdge+longEdge)/(2*longEdge))
	}
	targetWidth, targetHeight := maxOutputLongEdge, maxOutputLongEdge
	if width > height {
		targetHeight = shortSide(height)
	} else {
		targetWidth = shortSide(width)
	}
	scaled := image.NewRGBA(image.Rect(0, 0, targetWidth, targetHeight))
	draw.CatmullRom.Scale(scaled, scaled.Bounds(), img, bounds, draw.Src, nil)
	return scaled
}

// isOpaque reports whether every pixel is fully opaque. The decoder returns
// *image.RGBA or *image.RGBA64, both of which answer this directly; any other
// type counts as translucent so its alpha is kept.
func isOpaque(img image.Image) bool {
	opaque, ok := img.(interface{ Opaque() bool })
	return ok && opaque.Opaque()
}

// encode writes img as JPEG when it is opaque, else as PNG.
func encode(img image.Image, opaque bool) ([]byte, string, error) {
	var buffer bytes.Buffer
	if opaque {
		if err := jpeg.Encode(&buffer, img, &jpeg.Options{Quality: jpegQuality}); err != nil {
			return nil, "", fmt.Errorf("media: encoding JPEG: %w", err)
		}
		return buffer.Bytes(), "image/jpeg", nil
	}
	if err := png.Encode(&buffer, img); err != nil {
		return nil, "", fmt.Errorf("media: encoding PNG: %w", err)
	}
	return buffer.Bytes(), "image/png", nil
}

// IsAVIF reports whether data opens with an ISOBMFF ftyp box whose major
// brand or one of whose compatible brands is avif (still) or avis
// (sequence). Compatible brands are read only within the box's declared
// size, and only when the whole box is present.
func IsAVIF(data []byte) bool {
	if len(data) < 12 || string(data[4:8]) != "ftyp" {
		return false
	}
	if isAVIFBrand(data[8:12]) {
		return true
	}
	// Box header (size, type), major brand, minor version, then 4-byte
	// compatible brands up to the declared size.
	const compatibleBrandsStart = 16
	declaredSize := int64(binary.BigEndian.Uint32(data[0:4]))
	if declaredSize < compatibleBrandsStart || declaredSize > int64(len(data)) {
		return false
	}
	for offset := compatibleBrandsStart; offset+4 <= int(declaredSize); offset += 4 {
		if isAVIFBrand(data[offset : offset+4]) {
			return true
		}
	}
	return false
}

func isAVIFBrand(brand []byte) bool {
	return string(brand) == "avif" || string(brand) == "avis"
}
