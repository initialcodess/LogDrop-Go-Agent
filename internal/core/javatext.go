// Package core holds LogDrop's privacy and export rules, the same as the Java agent's core (the reference): which
// attributes may leave the process, the value-free SQL template, the recording limits, and the endpoint, credential
// and identity checks. The cases in conformance/privacy are shared by every LogDrop agent.
package core

import "unicode/utf8"

// The Java core measures and cuts text in UTF-16 code units (String#length, String#substring); Go strings are UTF-8,
// so lengths and cuts here count UTF-16 units too.

// Trim is java.lang.String#trim: it removes every character <= U+0020 at both ends (not U+00A0 or other spaces).
func Trim(value string) string {
	start, end := 0, len(value)
	for start < end && value[start] <= ' ' {
		start++
	}
	for end > start && value[end-1] <= ' ' {
		end--
	}
	return value[start:end]
}

// HasControl is Character.isISOControl for any character: U+0000..U+001F and U+007F..U+009F.
func HasControl(value string) bool {
	for _, r := range value {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return true
		}
	}
	return false
}

// IsBlank is String#isEmpty after String#trim.
func IsBlank(value string) bool { return len(Trim(value)) == 0 }

// UTF16Len is String#length: characters outside the Basic Multilingual Plane count twice. Invalid UTF-8 bytes count
// as one unit each (they become U+FFFD).
func UTF16Len(value string) int {
	n := 0
	for _, r := range value {
		if r > 0xFFFF {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// UTF16Prefix is String#substring(0, max) when the string is longer. A character that would be cut in half is left
// out (Java would keep half of it, which no encoder can send). Invalid UTF-8 becomes U+FFFD, so the result is always
// valid UTF-8.
func UTF16Prefix(value string, max int) string {
	if len(value) <= max && utf8.ValidString(value) {
		return value
	}
	units := 0
	out := make([]rune, 0, max)
	for _, r := range value {
		size := 1
		if r > 0xFFFF {
			size = 2
		}
		if units+size > max {
			break
		}
		units += size
		out = append(out, r)
	}
	return string(out)
}
