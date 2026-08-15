package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestAsyncImageDefaults(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	setDefaults()
	require.True(t, viper.GetBool("async_image.enabled"))
	require.Equal(t, 60, viper.GetInt("async_image.worker_concurrency"))
	require.Equal(t, 24, viper.GetInt("async_image.retention_hours"))
	require.True(t, viper.GetBool("image_storage.local_enabled"))
	require.Equal(t, 24, viper.GetInt("image_storage.retention_hours"))
}
