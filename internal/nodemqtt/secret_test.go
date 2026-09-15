package nodemqtt

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCommandCipherPersistsKeyAndDoesNotStorePlaintext(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "vocat.db")
	first, err := newCommandCipher(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(`{"action":"esim.profile.download","params":{"activation_code":"LPA:1$secret$matching"}}`)
	encrypted, err := first.encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encrypted, []byte("matching")) {
		t.Fatalf("encrypted command leaked activation material: %s", encrypted)
	}
	second, err := newCommandCipher(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := second.decrypt(encrypted)
	if err != nil || !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypt = %q, %v", decrypted, err)
	}
	info, err := os.Stat(databasePath + commandKeySuffix)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("key permissions = %o", info.Mode().Perm())
	}
}
