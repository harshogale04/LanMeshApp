package core

import "os"

func dirMkdirAll(dir string) error {
	return os.MkdirAll(dir, 0755)
}