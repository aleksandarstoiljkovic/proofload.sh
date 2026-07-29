package emit

import (
	"os"
	"path/filepath"
)

// writeAtomic writes data to path without ever exposing a partial file: it
// creates a temp file in the same directory, fsyncs it, then renames it into
// place (rename is atomic within a filesystem). Parent directories are created
// as needed. A failed write leaves the destination untouched.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if err := writeAndSync(tmp, data); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// writeAndSync writes all of data to f, flushes it to stable storage, and
// closes f. On any error f is closed before returning.
func writeAndSync(f *os.File, data []byte) error {
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
