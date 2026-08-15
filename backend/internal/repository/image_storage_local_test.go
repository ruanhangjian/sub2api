package repository

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalImageStorageSaveOpenAndCleanup(t *testing.T) {
	root := t.TempDir()
	storage, err := NewLocalImageStorage(root, "/v1/images/tasks", time.Hour, time.Hour)
	require.NoError(t, err)

	url, err := storage.Save(context.Background(), "images/imgtask_abc/0.png", "image/png", []byte("png-data"))
	require.NoError(t, err)
	require.Equal(t, "/v1/images/tasks/imgtask_abc/files/0.png", url)

	file, err := storage.Open(context.Background(), "images/imgtask_abc/0.png")
	require.NoError(t, err)
	data, err := io.ReadAll(file.Reader)
	require.NoError(t, err)
	require.NoError(t, file.Reader.Close())
	require.Equal(t, []byte("png-data"), data)

	target := filepath.Join(root, "images", "imgtask_abc", "0.png")
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(target, old, old))
	require.NoError(t, storage.CleanupExpired(context.Background(), time.Now()))
	_, err = os.Stat(target)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestLocalImageStorageRejectsTraversal(t *testing.T) {
	storage, err := NewLocalImageStorage(t.TempDir(), "", time.Hour, time.Hour)
	require.NoError(t, err)
	_, err = storage.Save(context.Background(), "../secret.png", "image/png", []byte("x"))
	require.Error(t, err)
	_, err = storage.Open(context.Background(), "images/imgtask_abc/../../secret.png")
	require.Error(t, err)
}

func TestLocalImageStorageRejectsSymlinkEscape(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	storage, err := NewLocalImageStorage(root, "", time.Hour, time.Hour)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "images"), 0o750))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "images", "imgtask_link")))

	_, err = storage.Save(context.Background(), "images/imgtask_link/0.png", "image/png", []byte("x"))
	require.Error(t, err)
	_, err = storage.Open(context.Background(), "images/imgtask_link/0.png")
	require.Error(t, err)
}
