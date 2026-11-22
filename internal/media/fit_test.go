package media

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"math/rand/v2"
	"testing"
	"time"

	"golang.org/x/image/webp"
)

// testdata/still.webp and testdata/animated.webp were made with Python PIL
// 13.0.0 (no Go WebP encoder exists):
//
//	random.seed(7)
//	im = Image.new("RGB", (240, 160))  # top-left 120x80 is (200, 40, 40), the rest random noise
//	im.save("still.webp", "WEBP", quality=90)  # lossy VP8, 23,684 bytes
//	a = Image.new("RGB", (16, 16), (255, 0, 0)); b = Image.new("RGB", (16, 16), (0, 0, 255))
//	a.save("animated.webp", "WEBP", save_all=True, append_images=[b], duration=100, loop=0, lossless=True)
//	# VP8X with the animation flag, ANIM, two ANMF frames, 140 bytes

// U1: an opaque JPEG over budget is re-encoded as a smaller JPEG that fits,
// keeping its 3:2 shape.
func TestFitStillShrinksOpaqueJPEGToBudget(t *testing.T) {
	source := encodeJPEGFixture(t, noiseImage(3000, 2000, 11))
	const budget = 300_000
	if len(source) <= budget {
		t.Fatalf("fixture: source is %d bytes, want over %d", len(source), budget)
	}

	out, mimeType, err := FitStill(boundedContext(t), source, "image/jpeg", budget, 4096)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
	}
	if len(out) > budget {
		t.Fatalf("output is %d bytes, want at most %d", len(out), budget)
	}
	bounds := decodeJPEGOutput(t, out).Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width >= 3000 {
		t.Errorf("output width = %d, want below the source's 3000", width)
	}
	// 3:2 within a pixel: |2·width − 3·height| ≤ 3.
	if skew := 2*width - 3*height; skew < -3 || skew > 3 {
		t.Errorf("output dimensions = %dx%d, want 3:2 within 1px", width, height)
	}
}

// U2: the long edge is capped at maxLongEdge even when the bytes already fit.
func TestFitStillCapsLongEdge(t *testing.T) {
	fill := color.NRGBA{R: 40, G: 120, B: 200, A: 255}
	source := encodePNGFixture(t, uniformImage(3000, 1000, fill))

	out, mimeType, err := FitStill(boundedContext(t), source, "image/png", generousMaxBytes, 1000)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg for an opaque image", mimeType)
	}
	if len(out) == 0 {
		t.Fatalf("output is empty")
	}
	decoded := decodeJPEGOutput(t, out)
	assertDimensions(t, decoded, 1000, 333)
	assertColourNear(t, "centre", nrgbaAt(decoded, 500, 166), fill, 6)
}

// U3: a translucent PNG over budget stays PNG and keeps its alpha.
func TestFitStillTranslucentPNGKeepsAlpha(t *testing.T) {
	solid := color.NRGBA{R: 30, G: 160, B: 90, A: 128}
	img := noiseImage(800, 800, 23)
	for i := 3; i < len(img.Pix); i += 4 {
		img.Pix[i] = 128
	}
	for y := 0; y < 400; y++ {
		for x := 0; x < 400; x++ {
			img.SetNRGBA(x, y, solid)
		}
	}
	source := encodePNGFixture(t, img)
	const budget = 200_000
	if len(source) <= budget {
		t.Fatalf("fixture: source is %d bytes, want over %d", len(source), budget)
	}

	out, mimeType, err := FitStill(boundedContext(t), source, "image/png", budget, 4096)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	if mimeType != "image/png" {
		t.Fatalf("mimeType = %q, want image/png for a translucent image", mimeType)
	}
	if len(out) > budget {
		t.Fatalf("output is %d bytes, want at most %d", len(out), budget)
	}
	decoded := decodePNGOutput(t, out)
	bounds := decoded.Bounds()
	if bounds.Dx() != bounds.Dy() {
		t.Errorf("output dimensions = %dx%d, want square", bounds.Dx(), bounds.Dy())
	}
	sample := nrgbaAt(decoded, bounds.Dx()/4, bounds.Dy()/4)
	assertAlphaNear(t, "top-left quadrant", sample, 128, 2)
	assertColourNear(t, "top-left quadrant", sample, solid, 4)
}

// U4: a static WebP over budget is decoded and re-encoded to fit.
func TestFitStillStaticWebP(t *testing.T) {
	source := readTestdata(t, "still.webp")
	const budget = 8_000
	if len(source) <= budget {
		t.Fatalf("fixture: source is %d bytes, want over %d", len(source), budget)
	}

	out, mimeType, err := FitStill(boundedContext(t), source, "image/webp", budget, 4096)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg for an opaque WebP", mimeType)
	}
	if len(out) > budget {
		t.Fatalf("output is %d bytes, want at most %d", len(out), budget)
	}
	decoded := decodeJPEGOutput(t, out)
	bounds := decoded.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if skew := 2*width - 3*height; skew < -3 || skew > 3 {
		t.Errorf("output dimensions = %dx%d, want 3:2 within 1px", width, height)
	}
	assertColourNear(t, "top-left quadrant", nrgbaAt(decoded, width/4, height/4), color.NRGBA{R: 200, G: 40, B: 40, A: 255}, 24)
}

// U5: animated GIF, WebP and PNG are refused with ErrAnimated, even when over
// budget, rather than flattened to one frame.
func TestFitStillRefusesAnimatedImages(t *testing.T) {
	palette := color.Palette{color.RGBA{R: 255, A: 255}, color.RGBA{B: 255, A: 255}}
	frame := func(index uint8) *image.Paletted {
		img := image.NewPaletted(image.Rect(0, 0, 64, 64), palette)
		for i := range img.Pix {
			img.Pix[i] = index
		}
		return img
	}
	twoFrameGIF := encodeGIFFixture(t, &gif.GIF{Image: []*image.Paletted{frame(0), frame(1)}, Delay: []int{10, 10}})
	apng := withAPNGFrames(t, encodePNGFixture(t, uniformImage(64, 64, color.NRGBA{R: 255, A: 255})), 64, 64)

	tests := []struct {
		name     string
		data     []byte
		mimeType string
	}{
		{name: "two-frame GIF", data: twoFrameGIF, mimeType: "image/gif"},
		{name: "animated WebP", data: readTestdata(t, "animated.webp"), mimeType: "image/webp"},
		{name: "APNG", data: apng, mimeType: "image/png"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := int64(len(test.data) - 1)
			_, _, err := FitStill(boundedContext(t), test.data, test.mimeType, budget, 4096)
			if !errors.Is(err, ErrAnimated) {
				t.Fatalf("err = %v, want ErrAnimated", err)
			}
		})
	}
}

// U5: a single-frame GIF over budget is fitted like any still image.
func TestFitStillStaticGIF(t *testing.T) {
	random := rand.New(rand.NewPCG(31, 37))
	img := image.NewPaletted(image.Rect(0, 0, 600, 400), paletteOf256Colours())
	for i := range img.Pix {
		img.Pix[i] = uint8(random.UintN(256))
	}
	source := encodeGIFFixture(t, &gif.GIF{Image: []*image.Paletted{img}, Delay: []int{0}})
	const budget = 60_000
	if len(source) <= budget {
		t.Fatalf("fixture: source is %d bytes, want over %d", len(source), budget)
	}

	out, mimeType, err := FitStill(boundedContext(t), source, "image/gif", budget, 4096)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	if len(out) > budget {
		t.Fatalf("output is %d bytes, want at most %d", len(out), budget)
	}
	bounds := decodeOutputImage(t, out, mimeType).Bounds()
	if skew := 2*bounds.Dx() - 3*bounds.Dy(); skew < -3 || skew > 3 {
		t.Errorf("output dimensions = %dx%d, want 3:2 within 1px", bounds.Dx(), bounds.Dy())
	}
}

// R7: a single-frame GIF whose frame is smaller than the logical screen and
// offset within it keeps the screen's shape and the frame's position.
func TestFitStillGIFFrameOffsetOnCanvas(t *testing.T) {
	frameColour := color.NRGBA{R: 220, G: 30, B: 30, A: 255}
	palette := color.Palette{color.RGBA{}, frameColour}
	frame := image.NewPaletted(image.Rect(600, 200, 800, 400), palette)
	for i := range frame.Pix {
		frame.Pix[i] = 1
	}
	source := encodeGIFFixture(t, &gif.GIF{
		Image:  []*image.Paletted{frame},
		Delay:  []int{0},
		Config: image.Config{ColorModel: palette, Width: 1000, Height: 500},
	})

	out, mimeType, err := FitStill(boundedContext(t), source, "image/gif", generousMaxBytes, 500)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	decoded := decodeOutputImage(t, out, mimeType)
	assertDimensions(t, decoded, 500, 250)
	// Screen (700,300), the frame's centre, lands at (350,150).
	assertColourNear(t, "frame centre", nrgbaAt(decoded, 350, 150), frameColour, 12)
	// Screen (200,250) is outside the frame.
	if outside := nrgbaAt(decoded, 100, 125); outside.A >= 128 && outside.R > 160 && outside.G < 90 && outside.B < 90 {
		t.Errorf("outside the frame: got %v, want something other than the frame colour", outside)
	}
}

func paletteOf256Colours() color.Palette {
	palette := make(color.Palette, 256)
	for i := range palette {
		palette[i] = color.RGBA{R: uint8(i), G: uint8(i * 7), B: uint8(i * 13), A: 255}
	}
	return palette
}

// U6/R4: declared dimensions over the caps are refused from the header alone;
// none of these fixtures carries any pixel data.
func TestFitStillDeclaredDimensionsOverCap(t *testing.T) {
	tests := []struct {
		name          string
		data          []byte
		mimeType      string
		width, height int
	}{
		{name: "PNG 8000x6000 is 48M pixels", data: pngHeaderOnly(8000, 6000), mimeType: "image/png", width: 8000, height: 6000},
		{name: "PNG 20000x20000", data: pngHeaderOnly(20000, 20000), mimeType: "image/png", width: 20000, height: 20000},
		{name: "PNG side 16385", data: pngHeaderOnly(16385, 1), mimeType: "image/png", width: 16385, height: 1},
		{name: "JPEG 8000x6000", data: jpegHeaderOnly(8000, 6000), mimeType: "image/jpeg", width: 8000, height: 6000},
		{name: "JPEG side 16385", data: jpegHeaderOnly(1, 16385), mimeType: "image/jpeg", width: 1, height: 16385},
		{name: "GIF 8000x6000", data: gifHeaderOnly(8000, 6000), mimeType: "image/gif", width: 8000, height: 6000},
		{name: "WebP 8000x6000", data: webpVP8XHeaderOnly(8000, 6000), mimeType: "image/webp", width: 8000, height: 6000},
		{name: "WebP side 16385", data: webpVP8XHeaderOnly(16385, 1), mimeType: "image/webp", width: 16385, height: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertDeclaredConfig(t, test.data, test.mimeType, test.width, test.height)
			_, _, err := FitStill(boundedContext(t), test.data, test.mimeType, 1000, 4096)
			if !errors.Is(err, ErrImageTooLarge) {
				t.Fatalf("err = %v, want ErrImageTooLarge", err)
			}
		})
	}
}

// assertDeclaredConfig checks that a header-only fixture declares the
// dimensions the test means it to.
func assertDeclaredConfig(t *testing.T, data []byte, mimeType string, width, height int) {
	t.Helper()
	var config image.Config
	var err error
	reader := bytes.NewReader(data)
	switch mimeType {
	case "image/png":
		config, err = png.DecodeConfig(reader)
	case "image/jpeg":
		config, err = jpeg.DecodeConfig(reader)
	case "image/gif":
		config, err = gif.DecodeConfig(reader)
	case "image/webp":
		config, err = webp.DecodeConfig(reader)
	}
	if err != nil {
		t.Fatalf("fixture: DecodeConfig: %v", err)
	}
	if config.Width != width || config.Height != height {
		t.Fatalf("fixture: declares %dx%d, want %dx%d", config.Width, config.Height, width, height)
	}
}

// U6/R4: a tall strip with one side over the AVIF path's 8192 but within
// 16384, and 12M pixels, is accepted and scaled to the long-edge cap.
func TestFitStillAcceptsTallStrip(t *testing.T) {
	fill := color.NRGBA{R: 240, G: 220, B: 180, A: 255}
	source := encodePNGFixture(t, uniformImage(1000, 12000, fill))

	out, mimeType, err := FitStill(boundedContext(t), source, "image/png", generousMaxBytes, 4096)
	if err != nil {
		t.Fatalf("FitStill: %v", err)
	}
	if mimeType != "image/jpeg" {
		t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
	}
	decoded := decodeJPEGOutput(t, out)
	assertDimensions(t, decoded, 341, 4096)
	assertColourNear(t, "centre", nrgbaAt(decoded, 170, 2048), fill, 6)
}

// U7/R2: a budget below any possible encode, even at a 1px long edge,
// ends in ErrEncodedTooLarge.
func TestFitStillCannotFit(t *testing.T) {
	source := encodePNGFixture(t, noiseImage(64, 64, 41))

	_, _, err := FitStill(boundedContext(t), source, "image/png", 50, 4096)
	if !errors.Is(err, ErrEncodedTooLarge) {
		t.Fatalf("err = %v, want ErrEncodedTooLarge", err)
	}
}

// U8: FitStill shares the single decode slot and gives up after decodeSlotWait.
func TestFitStillGivesUpWaitingForDecodeSlot(t *testing.T) {
	source := encodePNGFixture(t, uniformImage(100, 100, color.NRGBA{R: 120, G: 30, B: 220, A: 255}))

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

	_, _, err := FitStill(boundedContext(t), source, "image/png", generousMaxBytes, 50)
	if !errors.Is(err, ErrDecoderBusy) {
		t.Fatalf("with every decode slot taken: err = %v, want ErrDecoderBusy", err)
	}

	<-decodeSlots
	slotHeld = false

	out, mimeType, err := FitStill(boundedContext(t), source, "image/png", generousMaxBytes, 50)
	if err != nil {
		t.Fatalf("after the slot is free: FitStill: %v", err)
	}
	assertDimensions(t, decodeOutputImage(t, out, mimeType), 50, 50)
}

// U9/R6: EXIF orientation is applied before re-encoding, since the output
// carries no EXIF. The stored image is 400x200 with a solid colour in each
// quadrant; each row lists the stored quadrant that must appear in each
// corner of the displayed image.
func TestFitStillAppliesEXIFOrientation(t *testing.T) {
	red := color.NRGBA{R: 220, G: 30, B: 30, A: 255}
	green := color.NRGBA{R: 30, G: 200, B: 30, A: 255}
	blue := color.NRGBA{R: 30, G: 30, B: 220, A: 255}
	yellow := color.NRGBA{R: 230, G: 220, B: 30, A: 255}
	stored := image.NewNRGBA(image.Rect(0, 0, 400, 200))
	for y := 0; y < 200; y++ {
		for x := 0; x < 400; x++ {
			switch {
			case x < 200 && y < 100:
				stored.SetNRGBA(x, y, red) // top-left
			case y < 100:
				stored.SetNRGBA(x, y, green) // top-right
			case x < 200:
				stored.SetNRGBA(x, y, blue) // bottom-left
			default:
				stored.SetNRGBA(x, y, yellow) // bottom-right
			}
		}
	}
	plain := encodeJPEGFixture(t, stored)
	const budget = 40_000
	// Comment padding puts the source over budget while the re-encoded flat
	// quadrants fit at full size, so no extra shrink happens.
	padded := withJPEGCommentPadding(t, plain, 2*budget)
	if len(plain) >= budget/4 {
		t.Fatalf("fixture: unpadded source is %d bytes, want well under %d", len(plain), budget)
	}

	tests := []struct {
		orientation                                uint16
		width, height                              int
		topLeft, topRight, bottomLeft, bottomRight color.NRGBA
	}{
		{orientation: 1, width: 400, height: 200, topLeft: red, topRight: green, bottomLeft: blue, bottomRight: yellow},
		{orientation: 2, width: 400, height: 200, topLeft: green, topRight: red, bottomLeft: yellow, bottomRight: blue},
		{orientation: 3, width: 400, height: 200, topLeft: yellow, topRight: blue, bottomLeft: green, bottomRight: red},
		{orientation: 4, width: 400, height: 200, topLeft: blue, topRight: yellow, bottomLeft: red, bottomRight: green},
		{orientation: 5, width: 200, height: 400, topLeft: red, topRight: blue, bottomLeft: green, bottomRight: yellow},
		{orientation: 6, width: 200, height: 400, topLeft: blue, topRight: red, bottomLeft: yellow, bottomRight: green},
		{orientation: 7, width: 200, height: 400, topLeft: yellow, topRight: green, bottomLeft: blue, bottomRight: red},
		{orientation: 8, width: 200, height: 400, topLeft: green, topRight: yellow, bottomLeft: red, bottomRight: blue},
	}
	for _, test := range tests {
		t.Run("orientation "+string(rune('0'+test.orientation)), func(t *testing.T) {
			source := withEXIFOrientation(t, padded, test.orientation)
			if len(source) <= budget {
				t.Fatalf("fixture: source is %d bytes, want over %d", len(source), budget)
			}

			out, mimeType, err := FitStill(boundedContext(t), source, "image/jpeg", budget, 4096)
			if err != nil {
				t.Fatalf("FitStill: %v", err)
			}
			if mimeType != "image/jpeg" {
				t.Fatalf("mimeType = %q, want image/jpeg", mimeType)
			}
			if len(out) > budget {
				t.Fatalf("output is %d bytes, want at most %d", len(out), budget)
			}
			decoded := decodeJPEGOutput(t, out)
			assertDimensions(t, decoded, test.width, test.height)
			const inset = 20
			right, bottom := test.width-1-inset, test.height-1-inset
			assertColourNear(t, "top-left", nrgbaAt(decoded, inset, inset), test.topLeft, 12)
			assertColourNear(t, "top-right", nrgbaAt(decoded, right, inset), test.topRight, 12)
			assertColourNear(t, "bottom-left", nrgbaAt(decoded, inset, bottom), test.bottomLeft, 12)
			assertColourNear(t, "bottom-right", nrgbaAt(decoded, right, bottom), test.bottomRight, 12)
		})
	}
}
