package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelMonitorTimingThresholds(t *testing.T) {
	require.Equal(t, 15*time.Second, monitorDegradedThreshold)
	require.Equal(t, 60*time.Second, monitorResponseHeaderTimeout)
	require.Equal(t, 90*time.Second, monitorRequestTimeout)
	require.Greater(t, monitorRequestTimeout, monitorResponseHeaderTimeout)
}
