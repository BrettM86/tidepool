package media

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/gen2brain/avif"
)

// losslessOptions keeps fixture pixels exact so expected colours and alpha are literals.
var losslessOptions = avif.Options{
	Speed:             10,
	Lossless:          true,
	ChromaSubsampling: image.YCbCrSubsampleRatio444,
}

// fastOptions is for fixtures where only geometry matters.
var fastOptions = avif.Options{
	Quality:           60,
	QualityAlpha:      100,
	Speed:             10,
	ChromaSubsampling: image.YCbCrSubsampleRatio444,
}

func uniformImage(width, height int, fill color.NRGBA) *image.NRGBA {
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetNRGBA(x, y, fill)
		}
	}
	return img
}

// noiseImage returns opaque deterministic noise from a fixed seed.
func noiseImage(width, height int, seed uint64) *image.NRGBA {
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for i := 0; i < len(img.Pix); i += 4 {
		img.Pix[i] = uint8(random.UintN(256))
		img.Pix[i+1] = uint8(random.UintN(256))
		img.Pix[i+2] = uint8(random.UintN(256))
		img.Pix[i+3] = 255
	}
	return img
}

func encodeAVIF(t *testing.T, img image.Image, options avif.Options) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := avif.Encode(&buffer, img, options); err != nil {
		t.Fatalf("fixture: avif.Encode: %v", err)
	}
	return buffer.Bytes()
}

func encodeAnimatedAVIF(t *testing.T, frames []image.Image, options avif.Options) []byte {
	t.Helper()
	delays := make([]float64, len(frames))
	for i := range delays {
		delays[i] = 0.1
	}
	var buffer bytes.Buffer
	if err := avif.EncodeAll(&buffer, &avif.AVIF{Image: frames, Delay: delays}, options); err != nil {
		t.Fatalf("fixture: avif.EncodeAll: %v", err)
	}
	return buffer.Bytes()
}

func encodePNGFixture(t *testing.T, img image.Image) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := png.Encode(&buffer, img); err != nil {
		t.Fatalf("fixture: png.Encode: %v", err)
	}
	return buffer.Bytes()
}

// isobmffBox locates one box inside a file: data[start:end] is the whole box,
// data[start+headerLength:end] its payload.
type isobmffBox struct {
	boxType      string
	start        int
	headerLength int
	end          int
}

// childBoxes walks the boxes laid end to end in data[start:end].
func childBoxes(t *testing.T, data []byte, start, end int) []isobmffBox {
	t.Helper()
	var boxes []isobmffBox
	offset := start
	for offset+8 <= end {
		size := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		headerLength := 8
		switch size {
		case 1:
			size = int(binary.BigEndian.Uint64(data[offset+8 : offset+16]))
			headerLength = 16
		case 0:
			size = end - offset
		}
		if size < headerLength || offset+size > end {
			t.Fatalf("fixture: malformed box at offset %d (size %d)", offset, size)
		}
		boxes = append(boxes, isobmffBox{
			boxType:      string(data[offset+4 : offset+8]),
			start:        offset,
			headerLength: headerLength,
			end:          offset + size,
		})
		offset += size
	}
	return boxes
}

// findBoxes returns every box reached by following path from the top level,
// e.g. findBoxes(t, data, "meta", "iprp", "ipco", "ispe").
func findBoxes(t *testing.T, data []byte, path ...string) []isobmffBox {
	t.Helper()
	current := []isobmffBox{{boxType: "", start: 0, headerLength: 0, end: len(data)}}
	for _, boxType := range path {
		var next []isobmffBox
		for _, parent := range current {
			childStart := parent.start + parent.headerLength
			if parent.boxType == "meta" {
				childStart += 4 // FullBox version and flags precede the children.
			}
			for _, child := range childBoxes(t, data, childStart, parent.end) {
				if child.boxType == boxType {
					next = append(next, child)
				}
			}
		}
		current = next
	}
	if len(current) == 0 {
		t.Fatalf("fixture: no box at path %v", path)
	}
	return current
}

// withEmptyPitm replaces the primary-item box with one that has no payload and
// shrinks meta to match.
func withEmptyPitm(t *testing.T, data []byte) []byte {
	t.Helper()
	meta := findBoxes(t, data, "meta")[0]
	pitm := findBoxes(t, data, "meta", "pitm")[0]
	if meta.headerLength != 8 {
		t.Fatalf("fixture: meta uses a 64-bit size")
	}
	emptyPitm := []byte{0, 0, 0, 8, 'p', 'i', 't', 'm'}
	removed := (pitm.end - pitm.start) - len(emptyPitm)

	patched := make([]byte, 0, len(data)-removed)
	patched = append(patched, data[:pitm.start]...)
	patched = append(patched, emptyPitm...)
	patched = append(patched, data[pitm.end:]...)
	binary.BigEndian.PutUint32(patched[meta.start:], uint32(meta.end-meta.start-removed))
	return patched
}

// withOverflowingLargeSize inserts, right after ftyp, a box whose 64-bit size is
// 0x7ffffffffffffff0.
func withOverflowingLargeSize(t *testing.T, data []byte) []byte {
	t.Helper()
	ftyp := findBoxes(t, data, "ftyp")[0]
	badBox := []byte{0, 0, 0, 1, 'f', 'r', 'e', 'e', 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf0}

	patched := make([]byte, 0, len(data)+len(badBox))
	patched = append(patched, data[:ftyp.end]...)
	patched = append(patched, badBox...)
	patched = append(patched, data[ftyp.end:]...)
	return patched
}

// withDeclaredDimensions rewrites every ispe property to width×height; the
// compressed frame keeps its real size.
func withDeclaredDimensions(t *testing.T, data []byte, width, height uint32) []byte {
	t.Helper()
	patched := bytes.Clone(data)
	for _, ispe := range findBoxes(t, patched, "meta", "iprp", "ipco", "ispe") {
		payload := ispe.start + ispe.headerLength
		if ispe.end-payload < 12 {
			t.Fatalf("fixture: ispe payload too short")
		}
		binary.BigEndian.PutUint32(patched[payload+4:], width)
		binary.BigEndian.PutUint32(patched[payload+8:], height)
	}
	return patched
}

// withUnparseableItemLocations sets the iloc box to version 3, which the full
// decoder rejects while the header still declares the image dimensions. (Overwriting
// or truncating the mdat payload does not work: gen2brain/avif v0.6.0 swallows the
// frame-decode error and returns uninitialised output with a nil error.)
func withUnparseableItemLocations(t *testing.T, data []byte) []byte {
	t.Helper()
	patched := bytes.Clone(data)
	iloc := findBoxes(t, patched, "meta", "iloc")[0]
	patched[iloc.start+iloc.headerLength] = 3
	return patched
}

func decodeJPEGOutput(t *testing.T, out []byte) image.Image {
	t.Helper()
	img, err := jpeg.Decode(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("jpeg.Decode(output): %v", err)
	}
	return img
}

func decodePNGOutput(t *testing.T, out []byte) image.Image {
	t.Helper()
	img, err := png.Decode(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("png.Decode(output): %v", err)
	}
	return img
}

func nrgbaAt(img image.Image, x, y int) color.NRGBA {
	return color.NRGBAModel.Convert(img.At(x, y)).(color.NRGBA)
}

func assertColourNear(t *testing.T, label string, got, want color.NRGBA, tolerance int) {
	t.Helper()
	channels := [][3]int{
		{'R', int(got.R), int(want.R)},
		{'G', int(got.G), int(want.G)},
		{'B', int(got.B), int(want.B)},
	}
	for _, channel := range channels {
		difference := channel[1] - channel[2]
		if difference < -tolerance || difference > tolerance {
			t.Errorf("%s: channel %c = %d, want %d ±%d (got %v)", label, channel[0], channel[1], channel[2], tolerance, got)
		}
	}
}

func assertDimensions(t *testing.T, img image.Image, wantWidth, wantHeight int) {
	t.Helper()
	if got := img.Bounds(); got.Dx() != wantWidth || got.Dy() != wantHeight {
		t.Fatalf("output dimensions = %dx%d, want %dx%d", got.Dx(), got.Dy(), wantWidth, wantHeight)
	}
}

func encodeJPEGFixture(t *testing.T, img image.Image) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := jpeg.Encode(&buffer, img, nil); err != nil {
		t.Fatalf("fixture: jpeg.Encode: %v", err)
	}
	return buffer.Bytes()
}

// withRotation appends an irot property (angle × 90° anti-clockwise) to ipco,
// associates it with the primary item in ipma, and fixes the enclosing box sizes
// and the iloc extent offsets that point past the insertion.
func withRotation(t *testing.T, data []byte, angle byte) []byte {
	t.Helper()
	meta := findBoxes(t, data, "meta")[0]
	iprp := findBoxes(t, data, "meta", "iprp")[0]
	ipco := findBoxes(t, data, "meta", "iprp", "ipco")[0]
	ipma := findBoxes(t, data, "meta", "iprp", "ipma")[0]
	pitm := findBoxes(t, data, "meta", "pitm")[0]
	for _, box := range []isobmffBox{meta, iprp, ipco, ipma} {
		if box.headerLength != 8 {
			t.Fatalf("fixture: %s uses a 64-bit size", box.boxType)
		}
	}
	if ipma.end < ipco.end {
		t.Fatalf("fixture: ipma precedes ipco")
	}

	pitmPayload := pitm.start + pitm.headerLength
	if data[pitmPayload] != 0 {
		t.Fatalf("fixture: pitm version %d unsupported", data[pitmPayload])
	}
	primaryItem := binary.BigEndian.Uint16(data[pitmPayload+4:])

	// ipma version 0 with 1-byte property indices: entry = item_ID(2) count(1) indices(count).
	ipmaPayload := ipma.start + ipma.headerLength
	if data[ipmaPayload] != 0 || data[ipmaPayload+3]&1 != 0 {
		t.Fatalf("fixture: ipma version %d flags %d unsupported", data[ipmaPayload], data[ipmaPayload+3])
	}
	entryCount := int(binary.BigEndian.Uint32(data[ipmaPayload+4:]))
	offset := ipmaPayload + 8
	associationCountAt, indexInsertAt := -1, -1
	for i := 0; i < entryCount; i++ {
		item := binary.BigEndian.Uint16(data[offset:])
		count := int(data[offset+2])
		if item == primaryItem {
			associationCountAt = offset + 2
			indexInsertAt = offset + 3 + count
		}
		offset += 3 + count
	}
	if associationCountAt < 0 {
		t.Fatalf("fixture: ipma has no entry for primary item %d", primaryItem)
	}

	propertyCount := len(childBoxes(t, data, ipco.start+ipco.headerLength, ipco.end))
	rotationIndex := byte(propertyCount + 1)
	if rotationIndex > 0x7f {
		t.Fatalf("fixture: too many properties for a 1-byte index")
	}
	irot := []byte{0, 0, 0, 9, 'i', 'r', 'o', 't', angle & 0x3}
	const essential = 0x80
	inserted := len(irot) + 1

	patched := make([]byte, 0, len(data)+inserted)
	patched = append(patched, data[:ipco.end]...)
	patched = append(patched, irot...)
	patched = append(patched, data[ipco.end:indexInsertAt]...)
	patched = append(patched, essential|rotationIndex)
	patched = append(patched, data[indexInsertAt:]...)

	// ipma lies after ipco, so its bytes moved by len(irot).
	patched[associationCountAt+len(irot)]++
	binary.BigEndian.PutUint32(patched[ipma.start+len(irot):], uint32(ipma.end-ipma.start+1))
	growBox := func(box isobmffBox, by int) {
		binary.BigEndian.PutUint32(patched[box.start:], uint32(box.end-box.start+by))
	}
	growBox(meta, inserted)
	growBox(iprp, inserted)
	growBox(ipco, len(irot))

	shiftItemLocations(t, patched, ipco.end, inserted)
	return patched
}

// shiftItemLocations adds by to every file-offset extent in iloc that points at or
// past insertedAt. iloc must precede the insertion point and carry no base offsets.
func shiftItemLocations(t *testing.T, data []byte, insertedAt, by int) {
	t.Helper()
	iloc := findBoxes(t, data, "meta", "iloc")[0]
	if iloc.end > insertedAt {
		t.Fatalf("fixture: iloc follows the insertion point")
	}
	offset := iloc.start + iloc.headerLength
	version := data[offset]
	offset += 4
	offsetSize := int(data[offset] >> 4)
	lengthSize := int(data[offset] & 0xf)
	baseOffsetSize := int(data[offset+1] >> 4)
	indexSize := 0
	if version == 1 || version == 2 {
		indexSize = int(data[offset+1] & 0xf)
	}
	offset += 2

	readUint := func(at, size int) int {
		switch size {
		case 0:
			return 0
		case 4:
			return int(binary.BigEndian.Uint32(data[at:]))
		case 8:
			return int(binary.BigEndian.Uint64(data[at:]))
		}
		t.Fatalf("fixture: iloc field size %d unsupported", size)
		return 0
	}
	writeUint := func(at, size, value int) {
		switch size {
		case 4:
			binary.BigEndian.PutUint32(data[at:], uint32(value))
		case 8:
			binary.BigEndian.PutUint64(data[at:], uint64(value))
		}
	}

	var itemCount int
	if version < 2 {
		itemCount = int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
	} else {
		itemCount = int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
	}
	for i := 0; i < itemCount; i++ {
		if version < 2 {
			offset += 2
		} else {
			offset += 4
		}
		constructionMethod := 0
		if version == 1 || version == 2 {
			constructionMethod = int(data[offset+1] & 0xf)
			offset += 2
		}
		offset += 2 // data_reference_index
		if baseOffsetSize != 0 && readUint(offset, baseOffsetSize) != 0 {
			t.Fatalf("fixture: iloc base offsets unsupported")
		}
		offset += baseOffsetSize
		extentCount := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		for j := 0; j < extentCount; j++ {
			offset += indexSize
			if extentOffset := readUint(offset, offsetSize); constructionMethod == 0 && extentOffset >= insertedAt {
				writeUint(offset, offsetSize, extentOffset+by)
			}
			offset += offsetSize + lengthSize
		}
	}
}

// withTenBitCodecConfig marks every av1C property high bit depth and every
// pixi property 10 bits per channel. The AV1 payload stays 8-bit, but
// gen2brain/avif v0.6.0 picks its output type from the parsed depth, so the
// decode returns *image.RGBA64 — the shape a real 10-bit file decodes to.
func withTenBitCodecConfig(t *testing.T, data []byte) []byte {
	t.Helper()
	patched := bytes.Clone(data)
	for _, codecConfig := range findBoxes(t, patched, "meta", "iprp", "ipco", "av1C") {
		payload := codecConfig.start + codecConfig.headerLength
		if codecConfig.end-payload < 4 {
			t.Fatalf("fixture: av1C payload too short")
		}
		patched[payload+2] |= 0x40 // high_bitdepth
	}
	for _, pixelInformation := range findBoxes(t, patched, "meta", "iprp", "ipco", "pixi") {
		// FullBox version and flags, then num_channels, then one depth byte per channel.
		channels := pixelInformation.start + pixelInformation.headerLength + 4
		if pixelInformation.end-channels < 1 || pixelInformation.end-channels-1 < int(patched[channels]) {
			t.Fatalf("fixture: pixi payload too short")
		}
		for channel := range int(patched[channels]) {
			patched[channels+1+channel] = 10
		}
	}
	return patched
}

// readTestdata returns a committed fixture from testdata/.
func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return data
}

// decodeOutputImage decodes out with the decoder for mimeType, which must be
// image/jpeg or image/png.
func decodeOutputImage(t *testing.T, out []byte, mimeType string) image.Image {
	t.Helper()
	switch mimeType {
	case "image/jpeg":
		return decodeJPEGOutput(t, out)
	case "image/png":
		return decodePNGOutput(t, out)
	}
	t.Fatalf("output mimeType = %q, want image/jpeg or image/png", mimeType)
	return nil
}

func assertAlphaNear(t *testing.T, label string, got color.NRGBA, want uint8, tolerance int) {
	t.Helper()
	if difference := int(got.A) - int(want); difference < -tolerance || difference > tolerance {
		t.Errorf("%s: alpha = %d, want %d ±%d (got %v)", label, got.A, want, tolerance, got)
	}
}

func encodeGIFFixture(t *testing.T, animation *gif.GIF) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := gif.EncodeAll(&buffer, animation); err != nil {
		t.Fatalf("fixture: gif.EncodeAll: %v", err)
	}
	return buffer.Bytes()
}

// pngChunk returns one PNG chunk: length, type, data, CRC over type and data.
func pngChunk(chunkType string, data []byte) []byte {
	chunk := binary.BigEndian.AppendUint32(nil, uint32(len(data)))
	chunk = append(chunk, chunkType...)
	chunk = append(chunk, data...)
	return binary.BigEndian.AppendUint32(chunk, crc32.ChecksumIEEE(chunk[4:]))
}

var pngSignature = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}

// pngHeaderOnly is a PNG signature, an IHDR declaring width×height 8-bit
// RGBA, and IEND: no pixel data at all.
func pngHeaderOnly(width, height uint32) []byte {
	header := binary.BigEndian.AppendUint32(nil, width)
	header = binary.BigEndian.AppendUint32(header, height)
	header = append(header, 8, 6, 0, 0, 0)
	data := append(bytes.Clone(pngSignature), pngChunk("IHDR", header)...)
	return append(data, pngChunk("IEND", nil)...)
}

// jpegHeaderOnly is SOI, a baseline SOF0 declaring width×height with three
// components, an SOS header, and EOI: no tables or entropy-coded data.
func jpegHeaderOnly(width, height uint16) []byte {
	data := []byte{0xff, 0xd8, 0xff, 0xc0, 0x00, 0x11, 0x08}
	data = binary.BigEndian.AppendUint16(data, height)
	data = binary.BigEndian.AppendUint16(data, width)
	data = append(data, 3, 1, 0x11, 0, 2, 0x11, 1, 3, 0x11, 1)
	data = append(data, 0xff, 0xda, 0x00, 0x0c, 3, 1, 0x00, 2, 0x11, 3, 0x11, 0, 63, 0)
	return append(data, 0xff, 0xd9)
}

// gifHeaderOnly is a GIF89a header declaring a width×height logical screen
// with a two-colour global table, then the trailer: no image descriptor.
func gifHeaderOnly(width, height uint16) []byte {
	data := []byte("GIF89a")
	data = binary.LittleEndian.AppendUint16(data, width)
	data = binary.LittleEndian.AppendUint16(data, height)
	data = append(data, 0x80, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff)
	return append(data, 0x3b)
}

// webpVP8XHeaderOnly is a RIFF WEBP container holding only a VP8X chunk
// (no flags) declaring a width×height canvas.
func webpVP8XHeaderOnly(width, height uint32) []byte {
	vp8x := []byte{0, 0, 0, 0}
	vp8x = append(vp8x, byte(width-1), byte((width-1)>>8), byte((width-1)>>16))
	vp8x = append(vp8x, byte(height-1), byte((height-1)>>8), byte((height-1)>>16))
	data := []byte("RIFF")
	data = binary.LittleEndian.AppendUint32(data, uint32(4+8+len(vp8x)))
	data = append(data, "WEBPVP8X"...)
	data = binary.LittleEndian.AppendUint32(data, uint32(len(vp8x)))
	return append(data, vp8x...)
}

// withAPNGFrames turns a still PNG into a two-frame APNG: acTL and the first
// fcTL go before IDAT (so IDAT is frame 0), and after IDAT a second fcTL and
// an fdAT repeat the same pixels as frame 1. image/png ignores these
// ancillary chunks and decodes the still frame.
func withAPNGFrames(t *testing.T, data []byte, width, height uint32) []byte {
	t.Helper()
	if !bytes.HasPrefix(data, pngSignature) {
		t.Fatalf("fixture: not a PNG")
	}
	frameControl := func(sequence uint32) []byte {
		control := binary.BigEndian.AppendUint32(nil, sequence)
		control = binary.BigEndian.AppendUint32(control, width)
		control = binary.BigEndian.AppendUint32(control, height)
		control = binary.BigEndian.AppendUint32(control, 0)  // x offset
		control = binary.BigEndian.AppendUint32(control, 0)  // y offset
		control = binary.BigEndian.AppendUint16(control, 1)  // delay numerator
		control = binary.BigEndian.AppendUint16(control, 10) // delay denominator
		return append(control, 0, 0)                         // dispose, blend
	}
	animationControl := binary.BigEndian.AppendUint32(nil, 2) // frames
	animationControl = binary.BigEndian.AppendUint32(animationControl, 0)

	out := bytes.Clone(pngSignature)
	var pixelData []byte
	insertedBefore := false
	for offset := len(pngSignature); offset+12 <= len(data); {
		length := int(binary.BigEndian.Uint32(data[offset:]))
		chunkType := string(data[offset+4 : offset+8])
		end := offset + 12 + length
		if end > len(data) {
			t.Fatalf("fixture: truncated PNG chunk %s", chunkType)
		}
		switch chunkType {
		case "IDAT":
			if !insertedBefore {
				out = append(out, pngChunk("acTL", animationControl)...)
				out = append(out, pngChunk("fcTL", frameControl(0))...)
				insertedBefore = true
			}
			pixelData = append(pixelData, data[offset+8:offset+8+length]...)
		case "IEND":
			out = append(out, pngChunk("fcTL", frameControl(1))...)
			out = append(out, pngChunk("fdAT", append(binary.BigEndian.AppendUint32(nil, 2), pixelData...))...)
		}
		out = append(out, data[offset:end]...)
		offset = end
	}
	if !insertedBefore {
		t.Fatalf("fixture: PNG has no IDAT")
	}
	return out
}

// withEXIFOrientation inserts, right after SOI, an APP1 Exif segment whose
// big-endian TIFF IFD0 holds a single Orientation (0x0112) entry.
func withEXIFOrientation(t *testing.T, data []byte, orientation uint16) []byte {
	t.Helper()
	if len(data) < 2 || data[0] != 0xff || data[1] != 0xd8 {
		t.Fatalf("fixture: not a JPEG")
	}
	payload := []byte("Exif\x00\x00MM\x00\x2a")
	payload = binary.BigEndian.AppendUint32(payload, 8) // IFD0 offset
	payload = binary.BigEndian.AppendUint16(payload, 1) // entry count
	payload = binary.BigEndian.AppendUint16(payload, 0x0112)
	payload = binary.BigEndian.AppendUint16(payload, 3) // SHORT
	payload = binary.BigEndian.AppendUint32(payload, 1) // count
	payload = binary.BigEndian.AppendUint16(payload, orientation)
	payload = append(payload, 0, 0)                     // value padding
	payload = binary.BigEndian.AppendUint32(payload, 0) // no next IFD
	segment := []byte{0xff, 0xe1}
	segment = binary.BigEndian.AppendUint16(segment, uint16(len(payload)+2))
	segment = append(segment, payload...)

	out := append([]byte{0xff, 0xd8}, segment...)
	return append(out, data[2:]...)
}

// withJPEGCommentPadding inserts COM segments totalling at least padding
// bytes right after SOI, growing the file without changing its pixels.
func withJPEGCommentPadding(t *testing.T, data []byte, padding int) []byte {
	t.Helper()
	if len(data) < 2 || data[0] != 0xff || data[1] != 0xd8 {
		t.Fatalf("fixture: not a JPEG")
	}
	out := []byte{0xff, 0xd8}
	for written := 0; written < padding; {
		size := min(padding-written, 60000)
		out = append(out, 0xff, 0xfe)
		out = binary.BigEndian.AppendUint16(out, uint16(size+2))
		out = append(out, bytes.Repeat([]byte{'x'}, size)...)
		written += size
	}
	return append(out, data[2:]...)
}
