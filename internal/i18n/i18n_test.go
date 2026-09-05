package i18n

import "testing"

func TestDefaultIsChinese(t *testing.T) {
	Set("zh") // ensure a clean Chinese baseline regardless of test order
	if got := T("已启用"); got != "已启用" {
		t.Fatalf("zh: T(已启用) = %q, want 已启用", got)
	}
}

func TestEnglishTranslation(t *testing.T) {
	Set("en")
	defer Set("zh")
	if got := T("已启用"); got != "Enabled" {
		t.Fatalf("en: T(已启用) = %q, want Enabled", got)
	}
	if got := T("美国"); got != "United States" {
		t.Fatalf("en: T(美国) = %q, want United States", got)
	}
	// unknown strings fall back to the Chinese key unchanged
	if got := T("未收录的字符串"); got != "未收录的字符串" {
		t.Fatalf("en: unknown string = %q, want fallback unchanged", got)
	}
}

func TestTfInterpolation(t *testing.T) {
	Set("en")
	defer Set("zh")
	got := Tf("设备数量已达上限，最多只能添加 %d 台设备", 5)
	want := "Device limit reached; at most 5 devices can be added."
	if got != want {
		t.Fatalf("Tf = %q, want %q", got, want)
	}
}

func TestSetNormalizesUnknownToEnglish(t *testing.T) {
	Set("fr") // unsupported -> treated as English
	defer Set("zh")
	if Lang() != "en" {
		t.Fatalf("Lang after Set(fr) = %q, want en", Lang())
	}
}
