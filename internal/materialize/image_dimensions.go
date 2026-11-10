package materialize

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"

	"golang.org/x/image/webp"
)

// imageAspectRatio reads the header of stored image bytes and returns the
// social.coves.embed.images#aspectRatio for them, as displayed: a JPEG
// whose EXIF orientation turns it a quarter turn has its sides swapped.
// The decoder is chosen by the stored mime type, never by the bytes, so
// untrusted media cannot pick its own decoder (see media.TranscodeAVIF).
// It returns nil when the header or a JPEG's EXIF block cannot be read,
// and the image is then written without one.
func imageAspectRatio(mimeType string, data []byte) map[string]any {
	var decodeConfig func(io.Reader) (image.Config, error)
	switch mimeType {
	case "image/png":
		decodeConfig = png.DecodeConfig
	case "image/jpeg":
		decodeConfig = jpeg.DecodeConfig
	case "image/gif":
		decodeConfig = gif.DecodeConfig
	case "image/webp":
		decodeConfig = webp.DecodeConfig
	default:
		return nil
	}
	config, err := decodeConfig(bytes.NewReader(data))
	if err != nil || config.Width < 1 || config.Height < 1 {
		return nil
	}
	width, height := config.Width, config.Height
	if mimeType == "image/jpeg" {
		orientation, ok := jpegOrientation(data)
		if !ok {
			return nil
		}
		// Orientations 5 to 8 are the transposes and quarter turns.
		if orientation >= 5 {
			width, height = height, width
		}
	}
	return map[string]any{"width": int64(width), "height": int64(height)}
}

// jpegOrientation returns the EXIF Orientation tag of a JPEG's APP1 segment,
// or 1 (as stored) when there is no EXIF block or it has no Orientation. ok
// is false when the marker structure or the EXIF block is malformed, since
// the rotation the client applies is then unknown.
func jpegOrientation(data []byte) (orientation int, ok bool) {
	if len(data) < 2 || data[0] != 0xff || data[1] != 0xd8 { // SOI
		return 0, false
	}
	offset := 2
	for {
		if offset >= len(data) || data[offset] != 0xff {
			return 0, false
		}
		// Any number of 0xff fill bytes may precede a marker.
		for offset+1 < len(data) && data[offset+1] == 0xff {
			offset++
		}
		if offset+4 > len(data) {
			return 0, false
		}
		marker := data[offset+1]
		if marker == 0xda || marker == 0xd9 {
			return 1, true // start of scan or end of image: no EXIF before the pixels
		}
		length := int(binary.BigEndian.Uint16(data[offset+2:]))
		if length < 2 || offset+2+length > len(data) {
			return 0, false
		}
		segment := data[offset+4 : offset+2+length]
		if marker == 0xe1 && bytes.HasPrefix(segment, []byte("Exif\x00\x00")) { // APP1
			return tiffOrientation(segment[6:])
		}
		offset += 2 + length
	}
}

// tiffOrientation reads the Orientation entry (tag 0x0112, one SHORT) of IFD0
// in a TIFF structure: 1 when there is none, ok false when it is malformed.
func tiffOrientation(tiff []byte) (orientation int, ok bool) {
	if len(tiff) < 8 {
		return 0, false
	}
	var order binary.ByteOrder
	switch string(tiff[:4]) {
	case "II*\x00":
		order = binary.LittleEndian
	case "MM\x00*":
		order = binary.BigEndian
	default:
		return 0, false
	}
	// Compared without adding to the offset, which may be near the int limit.
	ifd := uint64(order.Uint32(tiff[4:]))
	if ifd < 8 || ifd > uint64(len(tiff)-2) {
		return 0, false
	}
	entries := int(order.Uint16(tiff[ifd:]))
	const entrySize = 12 // tag, type, count, value
	for i := 0; i < entries; i++ {
		entry := int(ifd) + 2 + i*entrySize
		if entry+entrySize > len(tiff) {
			return 0, false
		}
		if order.Uint16(tiff[entry:]) != 0x0112 {
			continue
		}
		const typeShort = 3
		if order.Uint16(tiff[entry+2:]) != typeShort || order.Uint32(tiff[entry+4:]) != 1 {
			return 0, false
		}
		orientation := int(order.Uint16(tiff[entry+8:]))
		if orientation < 1 || orientation > 8 {
			return 0, false
		}
		return orientation, true
	}
	return 1, true
}
