package device

import "fmt"

// parseProfilesInfoFresh requires an explicit successful response and complete
// BER. The UI parser remains tolerant of legacy metadata and recovery caches.
func parseProfilesInfoFresh(payload []byte) ([]EsimProfile, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty GetProfilesInfo response")
	}
	if err := validateProfileBER(payload, 0); err != nil {
		return nil, err
	}
	roots := derParse(payload)
	if len(roots) != 1 {
		return nil, fmt.Errorf("unexpected GetProfilesInfo response roots")
	}
	root := roots[0]
	var records []*derNode
	switch root.tag {
	case 0xbf2d, 0xbf3d:
		if len(root.children) == 1 && (root.children[0].tag == 0xa0 || root.children[0].tag == 0x30) {
			records = root.children[0].children
		} else if len(root.children) > 0 {
			records = root.children
		} else {
			return nil, fmt.Errorf("GetProfilesInfo response has no success branch")
		}
	case 0xa0:
		records = root.children
	default:
		return nil, fmt.Errorf("unexpected GetProfilesInfo response tag")
	}
	for _, record := range records {
		if record.tag != 0xe3 {
			return nil, fmt.Errorf("GetProfilesInfo returned an error or unexpected list item")
		}
		if !validProfileICCID(decodeICCID(derValue(record.children, 0x5a))) {
			return nil, fmt.Errorf("GetProfilesInfo profile is missing a valid ICCID")
		}
	}
	return parseProfilesInfo(payload), nil
}

// Validate lengths before using the existing tolerant decoder. Inspect only
// constructed values; icons and other primitive byte strings are opaque.
func validateProfileBER(data []byte, depth int) error {
	if depth > 32 {
		return fmt.Errorf("GetProfilesInfo nesting is too deep")
	}
	for offset := 0; offset < len(data); {
		start := offset
		first := data[offset]
		offset++
		if first&0x1f == 0x1f {
			ended := false
			for n := 0; n < 4 && offset < len(data); n++ {
				b := data[offset]
				offset++
				if b&0x80 == 0 {
					ended = true
					break
				}
			}
			if !ended {
				return fmt.Errorf("invalid GetProfilesInfo tag")
			}
		}
		if offset >= len(data) {
			return fmt.Errorf("truncated GetProfilesInfo length")
		}
		size := int(data[offset])
		offset++
		if size&0x80 != 0 {
			count := size & 0x7f
			if count == 0 || count > 4 || count > len(data)-offset {
				return fmt.Errorf("invalid GetProfilesInfo length")
			}
			size = 0
			for i := 0; i < count; i++ {
				size = size<<8 | int(data[offset])
				offset++
			}
		}
		if size < 0 || size > len(data)-offset {
			return fmt.Errorf("truncated GetProfilesInfo value")
		}
		if first&0x20 != 0 {
			if err := validateProfileBER(data[offset:offset+size], depth+1); err != nil {
				return err
			}
		}
		offset += size
		if offset <= start {
			return fmt.Errorf("invalid GetProfilesInfo element")
		}
	}
	return nil
}
