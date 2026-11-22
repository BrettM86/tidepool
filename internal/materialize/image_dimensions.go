package materialize

import (
	"bytes"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"

	"golang.org/x/image/webp"

	"tidepool/internal/media"
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

// jpegOrientation and tiffOrientation are the EXIF readers media.FitStill
// uses, so the aspect ratio a record carries and the pixels a fitted image
// stores agree on the rotation.
var (
	jpegOrientation = media.JPEGOrientation
	tiffOrientation = media.TIFFOrientation
)
