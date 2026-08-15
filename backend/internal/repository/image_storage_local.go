package repository

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"go.uber.org/zap"
)

type LocalImageStorage struct {
	root            string
	baseURL         string
	retention       time.Duration
	cleanupInterval time.Duration
}

func NewLocalImageStorage(directory, baseURL string, retention, cleanupInterval time.Duration) (*LocalImageStorage, error) {
	if strings.TrimSpace(directory) == "" {
		directory = "./data/image-storage"
	}
	root, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("resolve local image directory: %w", err)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create local image directory: %w", err)
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "/v1/images/tasks"
	}
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	if cleanupInterval <= 0 {
		cleanupInterval = time.Hour
	}
	s := &LocalImageStorage{root: root, baseURL: baseURL, retention: retention, cleanupInterval: cleanupInterval}
	go s.runCleanup()
	return s, nil
}

func (s *LocalImageStorage) Save(ctx context.Context, key, _ string, data []byte) (string, error) {
	relative, err := validateLocalImageKey(key)
	if err != nil {
		return "", err
	}
	target, err := localImageTarget(s.root, relative)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return "", err
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil || !localPathWithinRoot(s.root, resolvedParent) {
		return "", errors.New("local image parent escapes storage root")
	}
	target = filepath.Join(resolvedParent, filepath.Base(target))
	tmp, err := os.CreateTemp(filepath.Dir(target), ".image-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = tmp.Close(); _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o640); err != nil {
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, target); err != nil {
		return "", err
	}
	parts := strings.Split(relative, "/")
	if len(parts) < 2 {
		return "", errors.New("local image key does not contain a task directory")
	}
	taskID, filename := parts[len(parts)-2], parts[len(parts)-1]
	return s.baseURL + "/" + url.PathEscape(taskID) + "/files/" + url.PathEscape(filename), nil
}

func (s *LocalImageStorage) Open(ctx context.Context, key string) (*service.ImageStoredFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	relative, err := validateLocalImageKey(key)
	if err != nil {
		return nil, err
	}
	target, err := localImageTarget(s.root, relative)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(target)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, os.ErrNotExist
	}
	resolved, err := filepath.EvalSymlinks(target)
	if err != nil || !localPathWithinRoot(s.root, resolved) {
		return nil, os.ErrNotExist
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, err
	}
	return &service.ImageStoredFile{Reader: file, Name: info.Name(), ModTime: info.ModTime()}, nil
}

func (s *LocalImageStorage) CleanupExpired(ctx context.Context, now time.Time) error {
	cutoff := now.Add(-s.retention)
	return filepath.WalkDir(s.root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return nil
	})
}

func (s *LocalImageStorage) runCleanup() {
	// Run once at startup so expired files are not retained until the first save.
	if err := s.CleanupExpired(context.Background(), time.Now()); err != nil {
		logger.L().Warn("image_storage.local_cleanup_failed", zap.Error(err))
	}
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()
	for now := range ticker.C {
		if err := s.CleanupExpired(context.Background(), now); err != nil {
			logger.L().Warn("image_storage.local_cleanup_failed", zap.Error(err))
		}
	}
}

func validateLocalImageKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") {
		return "", errors.New("invalid local image key")
	}
	cleaned := path.Clean(key)
	if cleaned == "." || cleaned != key || strings.HasPrefix(cleaned, "../") {
		return "", errors.New("invalid local image key")
	}
	return cleaned, nil
}

func localImageTarget(root, relative string) (string, error) {
	target := filepath.Join(root, filepath.FromSlash(relative))
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("local image path escapes storage root")
	}
	return target, nil
}

func localPathWithinRoot(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

var _ service.ImageStorage = (*LocalImageStorage)(nil)
var _ service.ImageStorageReader = (*LocalImageStorage)(nil)
