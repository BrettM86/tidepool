package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"runtime/debug"

	"golang.org/x/image/draw"
	"golang.org/x/image/webp"
)

const (
	// fitMaxDeclaredSide is FitStill's cap on either declared side. It is
	// twice the AVIF path's maxDeclaredSide so tall comic strips and long
	// screenshots (1000x12000, say) still fit; maxDeclaredPixels still
	// bounds the decode.
	fitMaxDeclaredSide = 16384
	// fitShrinkFactor scales the long edge down between attempts when no
	// encoding of the current size fits the byte cap.
	fitShrinkFactor = 0.75
)

// fitJPEGQualities are tried in order at each size; a PNG has one encoding.
var fitJPEGQualities = []int{85, 75, 60}

// ErrAnimated: the image has more than one frame (a GIF with several
// frames, a WebP with the animation flag or ANIM/ANMF chunks, or an APNG),
// and FitStill refuses it rather than keep only the first frame.
var ErrAnimated = errors.New("media: animated image")

// FitStill re-encodes a still JPEG, PNG, GIF or WebP so it fits maxBytes and
// its long edge is at most maxLongEdge pixels (no cap when maxLongEdge <= 0),
// never upscaling. The long edge is capped even when data is already under
// maxBytes. A JPEG's EXIF
// orientation is applied to the pixels, since the output carries no EXIF; a
// GIF's first frame is drawn on its logical screen. The output is JPEG when
// every pixel is opaque, tried at qualities 85, 75 and 60, else PNG. While
// no encoding fits, the long edge shrinks by a quarter and the encodings are
// tried again, down to a one-pixel long edge.
//
// The declared dimensions are read before any pixel is decoded: a side over
// 16384 or more than 40 million pixels is refused with ErrImageTooLarge. The
// decode then waits for the process-wide decode slot TranscodeAVIF uses,
// giving up after decodeSlotWait with ErrDecoderBusy, or earlier with ctx's
// error. Other errors wrap ErrAnimated, ErrUndecodable (including a
// mimeType other than the four above) or ErrEncodedTooLarge.
func FitStill(ctx context.Context, data []byte, mimeType string, maxBytes int64, maxLongEdge int) ([]byte, string, error) {
	var decodeConfig func(io.Reader) (image.Config, error)
	var decode func([]byte, image.Config) (image.Image, error)
	var animated func([]byte) bool
	switch mimeType {
	case "image/jpeg":
		decodeConfig, decode, animated = jpeg.DecodeConfig, decodeJPEG, func([]byte) bool { return false }
	case "image/png":
		decodeConfig, decode, animated = png.DecodeConfig, decodeReader(png.Decode), isAnimatedPNG
	case "image/gif":
		decodeConfig, decode, animated = gif.DecodeConfig, decodeGIFOnScreen, isAnimatedGIF
	case "image/webp":
		decodeConfig, decode, animated = webp.DecodeConfig, decodeReader(webp.Decode), isAnimatedWebP
	default:
		return nil, "", fmt.Errorf("%w: cannot fit %q", ErrUndecodable, mimeType)
	}
	if animated(data) {
		return nil, "", fmt.Errorf("%w: %s", ErrAnimated, mimeType)
	}
	config, err := decodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, "", fmt.Errorf("%w: reading %s dimensions: %w", ErrUndecodable, mimeType, err)
	}
	if config.Width <= 0 || config.Height <= 0 {
		return nil, "", fmt.Errorf("%w: declared dimensions %dx%d", ErrUndecodable, config.Width, config.Height)
	}
	declaredPixels := int64(config.Width) * int64(config.Height)
	if config.Width > fitMaxDeclaredSide || config.Height > fitMaxDeclaredSide || declaredPixels > maxDeclaredPixels {
		return nil, "", fmt.Errorf("%w: declared dimensions %dx%d", ErrImageTooLarge, config.Width, config.Height)
	}

	if err := acquireDecodeSlot(ctx); err != nil {
		return nil, "", err
	}
	defer func() { <-decodeSlots }()
	// As in TranscodeAVIF: a cancelled caller must never start a decode.
	if err := ctx.Err(); err != nil {
		return nil, "", fmt.Errorf("media: cancelled before decoding: %w", err)
	}

	out, outMime, err := decodeAndFit(ctx, data, config, decode, maxBytes, maxLongEdge, mimeType == "image/jpeg")
	if declaredPixels > releaseMemoryAbovePixels {
		debug.FreeOSMemory()
	}
	return out, outMime, err
}

// decodeAndFit decodes data and fits it, keeping the decoded image inside
// this call so its buffers are garbage once it returns (see decodeAndEncode).
func decodeAndFit(ctx context.Context, data []byte, config image.Config, decode func([]byte, image.Config) (image.Image, error), maxBytes int64, maxLongEdge int, isJPEG bool) ([]byte, string, error) {
	decoded, err := decode(data, config)
	if err != nil {
		return nil, "", err
	}
	orientation := 1
	if isJPEG {
		// A malformed EXIF block leaves the rotation unknown; the pixels
		// are kept as stored, as most viewers show them.
		if read, ok := JPEGOrientation(data); ok {
			orientation = read
		}
	}
	opaque := isOpaque(decoded)
	bounds := decoded.Bounds()
	longEdge := max(bounds.Dx(), bounds.Dy())
	if maxLongEdge > 0 {
		longEdge = max(1, min(longEdge, maxLongEdge))
	}
	// Each smaller size is scaled from the long-edge-capped, oriented image,
	// not the full decode, so a large source is resampled and rotated once.
	capped := applyOrientation(scaleToLongEdge(decoded, longEdge), orientation)
	for {
		if err := ctx.Err(); err != nil {
			return nil, "", fmt.Errorf("media: cancelled while fitting: %w", err)
		}
		out, mimeType, err := encodeWithin(scaleToLongEdge(capped, longEdge), opaque, maxBytes)
		if err != nil {
			return nil, "", err
		}
		if out != nil {
			return out, mimeType, nil
		}
		if longEdge == 1 {
			return nil, "", fmt.Errorf("%w: no encoding fits %d bytes, even at a 1px long edge", ErrEncodedTooLarge, maxBytes)
		}
		longEdge = max(1, int(float64(longEdge)*fitShrinkFactor))
	}
}

// encodeWithin returns the first encoding of img that fits maxBytes: JPEG at
// each of fitJPEGQualities when opaque, else PNG. It returns nil bytes and
// no error when none fits.
func encodeWithin(img image.Image, opaque bool, maxBytes int64) ([]byte, string, error) {
	if !opaque {
		out, err := encodePNG(img)
		if err != nil || int64(len(out)) > maxBytes {
			return nil, "", err
		}
		return out, "image/png", nil
	}
	for _, quality := range fitJPEGQualities {
		out, err := encodeJPEG(img, quality)
		if err != nil {
			return nil, "", err
		}
		if int64(len(out)) <= maxBytes {
			return out, "image/jpeg", nil
		}
	}
	return nil, "", nil
}

// decodeReader adapts a stdlib-style decoder, turning a panic on crafted
// input into ErrUndecodable.
func decodeReader(decoder func(io.Reader) (image.Image, error)) func([]byte, image.Config) (image.Image, error) {
	return func(data []byte, _ image.Config) (img image.Image, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("%w: decoder panic: %v", ErrUndecodable, recovered)
			}
		}()
		img, err = decoder(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUndecodable, err)
		}
		return img, nil
	}
}

var decodeJPEG = decodeReader(jpeg.Decode)

// decodeGIFOnScreen decodes a GIF's first frame and draws it at its offset
// on a transparent canvas the size of the logical screen, which is what a
// viewer shows; the frame alone may be smaller and offset.
func decodeGIFOnScreen(data []byte, config image.Config) (image.Image, error) {
	frame, err := decodeReader(gif.Decode)(data, config)
	if err != nil {
		return nil, err
	}
	canvas := image.NewNRGBA(image.Rect(0, 0, config.Width, config.Height))
	draw.Draw(canvas, frame.Bounds(), frame, frame.Bounds().Min, draw.Src)
	return canvas, nil
}

// applyOrientation returns img turned and mirrored as EXIF orientation 1-8
// asks, so it displays upright without the tag. Orientation 1 (or any value
// outside 2-8) returns img unchanged.
func applyOrientation(img *image.RGBA, orientation int) *image.RGBA {
	if orientation < 2 || orientation > 8 {
		return img
	}
	width, height := img.Bounds().Dx(), img.Bounds().Dy()
	outWidth, outHeight := width, height
	if orientation >= 5 {
		outWidth, outHeight = height, width
	}
	// source maps a displayed pixel to the stored one.
	source := func(x, y int) (int, int) {
		switch orientation {
		case 2: // mirrored horizontally
			return width - 1 - x, y
		case 3: // turned 180°
			return width - 1 - x, height - 1 - y
		case 4: // mirrored vertically
			return x, height - 1 - y
		case 5: // transposed
			return y, x
		case 6: // turned 90° clockwise
			return y, height - 1 - x
		case 7: // transversed
			return width - 1 - y, height - 1 - x
		default: // 8: turned 90° anticlockwise
			return width - 1 - y, x
		}
	}
	out := image.NewRGBA(image.Rect(0, 0, outWidth, outHeight))
	for y := range outHeight {
		for x := range outWidth {
			sourceX, sourceY := source(x, y)
			from := img.PixOffset(img.Rect.Min.X+sourceX, img.Rect.Min.Y+sourceY)
			copy(out.Pix[out.PixOffset(x, y):][:4], img.Pix[from:from+4])
		}
	}
	return out
}

// isAnimatedPNG reports whether an acTL chunk precedes the first IDAT,
// which makes the file an APNG. An acTL after IDAT is ignored by APNG
// decoders, so the file is still.
func isAnimatedPNG(data []byte) bool {
	const signatureLength = 8
	for offset := signatureLength; offset+8 <= len(data); {
		length := uint64(binary.BigEndian.Uint32(data[offset:]))
		switch string(data[offset+4 : offset+8]) {
		case "acTL":
			return true
		case "IDAT", "IEND":
			return false
		}
		// Length, type, data, CRC; compared without overflowing int.
		next := uint64(offset) + 12 + length
		if next > uint64(len(data)) {
			return false
		}
		offset = int(next)
	}
	return false
}

// isAnimatedWebP reports whether a RIFF WEBP file's VP8X chunk sets the
// animation flag or the file carries an ANIM or ANMF chunk.
func isAnimatedWebP(data []byte) bool {
	if len(data) < 12 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WEBP" {
		return false
	}
	const animationFlag = 0x02
	for offset := 12; offset+8 <= len(data); {
		fourCC := string(data[offset : offset+4])
		size := uint64(binary.LittleEndian.Uint32(data[offset+4:]))
		switch fourCC {
		case "ANIM", "ANMF":
			return true
		case "VP8X":
			if offset+9 <= len(data) && data[offset+8]&animationFlag != 0 {
				return true
			}
		}
		// Chunk header, payload, and a pad byte when the size is odd.
		next := uint64(offset) + 8 + size + size%2
		if next > uint64(len(data)) {
			return false
		}
		offset = int(next)
	}
	return false
}

// isAnimatedGIF walks a GIF's blocks without decoding pixels and reports
// whether it holds more than one image. A structure it cannot follow stops
// the walk: the decoder reads only the first frame anyway.
func isAnimatedGIF(data []byte) bool {
	const headerLength = 13 // signature, version, logical screen descriptor
	if len(data) < headerLength {
		return false
	}
	offset := headerLength
	if flags := data[10]; flags&0x80 != 0 {
		offset += 3 << ((flags & 0x07) + 1)
	}
	// skipSubBlocks returns the offset past a run of data sub-blocks, or -1.
	skipSubBlocks := func(offset int) int {
		for offset < len(data) {
			size := int(data[offset])
			offset++
			if size == 0 {
				return offset
			}
			offset += size
		}
		return -1
	}
	frames := 0
	for offset >= 0 && offset < len(data) {
		switch data[offset] {
		case 0x21: // extension: introducer, label, sub-blocks
			offset = skipSubBlocks(offset + 2)
		case 0x2c: // image descriptor
			frames++
			if frames > 1 {
				return true
			}
			const descriptorLength = 10
			if offset+descriptorLength > len(data) {
				return false
			}
			flags := data[offset+9]
			offset += descriptorLength
			if flags&0x80 != 0 {
				offset += 3 << ((flags & 0x07) + 1)
			}
			offset = skipSubBlocks(offset + 1) // past the LZW minimum code size
		default: // trailer, or a structure the walk cannot follow
			return false
		}
	}
	return false
}
