package judger

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func CopyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := CopyDir(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		if err := CopyFile(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func InteractiveBroadcastDir(submissionRoot, allocationID string) string {
	return filepath.Join(submissionRoot, "sbcast", allocationID)
}

func InteractiveBroadcastContainerRelativePath(destination string) (string, error) {
	destination = strings.TrimSpace(strings.ReplaceAll(destination, "\\", "/"))
	if destination == "" {
		return "", fmt.Errorf("destination path is required")
	}

	if strings.HasPrefix(destination, "/") {
		cleaned := path.Clean(destination)
		if cleaned == "/" || cleaned == "." {
			return "", fmt.Errorf("destination path must name a file")
		}
		return strings.TrimPrefix(cleaned, "/"), nil
	}

	cleaned := path.Clean(destination)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("relative destination path must stay under /mnt/work")
	}
	return strings.TrimPrefix(path.Join("/mnt/work", cleaned), "/"), nil
}

func WriteInteractiveBroadcastFile(submissionRoot, allocationID, destination string, data []byte) (string, error) {
	relativePath, err := InteractiveBroadcastContainerRelativePath(destination)
	if err != nil {
		return "", err
	}
	root := InteractiveBroadcastDir(submissionRoot, allocationID)
	target := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return "", err
	}
	if err := os.WriteFile(target, data, 0644); err != nil {
		return "", err
	}
	return "/" + relativePath, nil
}
