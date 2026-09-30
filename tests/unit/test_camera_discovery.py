"""Tests for ONVIF camera discovery using WS-Discovery."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from vector.runtime.camera_discovery import discover_onvif_cameras


def test_discover_onvif_cameras_no_response() -> None:
    """Test discovery when no cameras respond."""
    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        mock_wsd.searchServices.return_value = []

        result = discover_onvif_cameras(timeout=0.1, max_retries=1)

        assert result == []
        mock_wsd.start.assert_called_once()
        mock_wsd.searchServices.assert_called_once_with(timeout=0.1, types=["dn:NetworkVideoTransmitter"])
        mock_wsd.stop.assert_called_once()


def test_discover_onvif_cameras_single_camera() -> None:
    """Test discovery when one camera responds."""
    # Mock service with XAddrs
    mock_service = Mock()
    mock_service.getXAddrs.return_value = ["http://192.168.0.100:2020/onvif/device_service"]

    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        mock_wsd.searchServices.return_value = [mock_service]

        result = discover_onvif_cameras(timeout=0.1, max_retries=1)

        assert len(result) == 1
        assert result[0]["ip"] == "192.168.0.100"
        assert result[0]["onvif_port"] == 2020
        assert result[0]["hostname"] is None


def test_discover_onvif_cameras_multiple_cameras() -> None:
    """Test discovery when multiple cameras respond."""
    # Mock services with XAddrs
    mock_service1 = Mock()
    mock_service1.getXAddrs.return_value = ["http://192.168.0.100:2020/onvif/device_service"]
    mock_service2 = Mock()
    mock_service2.getXAddrs.return_value = ["http://192.168.0.101:2020/onvif/device_service"]

    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        mock_wsd.searchServices.return_value = [mock_service1, mock_service2]

        result = discover_onvif_cameras(timeout=0.1, max_retries=1)

        assert len(result) == 2
        assert result[0]["ip"] == "192.168.0.100"
        assert result[0]["onvif_port"] == 2020
        assert result[1]["ip"] == "192.168.0.101"
        assert result[1]["onvif_port"] == 2020


def test_discover_onvif_cameras_duplicate_ips() -> None:
    """Test discovery handles duplicate IP addresses from same camera."""
    # Mock services with same IP but different XAddrs (same camera, multiple services)
    mock_service1 = Mock()
    mock_service1.getXAddrs.return_value = ["http://192.168.0.100:2020/onvif/device_service"]
    mock_service2 = Mock()
    mock_service2.getXAddrs.return_value = ["http://192.168.0.100:2020/onvif/media_service1"]

    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        mock_wsd.searchServices.return_value = [mock_service1, mock_service2]

        result = discover_onvif_cameras(timeout=0.1, max_retries=1)

        # Should deduplicate by IP
        assert len(result) == 1
        assert result[0]["ip"] == "192.168.0.100"
        assert result[0]["onvif_port"] == 2020


def test_discover_onvif_cameras_malformed_xaddr() -> None:
    """Test discovery handles malformed XAddrs gracefully."""
    # Mock service with invalid XAddr
    mock_service = Mock()
    mock_service.getXAddrs.return_value = ["not-a-valid-url"]

    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        mock_wsd.searchServices.return_value = [mock_service]

        result = discover_onvif_cameras(timeout=0.1, max_retries=1)

        # Should skip malformed XAddr and return empty list
        assert result == []


def test_discover_onvif_cameras_timeout_retry() -> None:
    """Test discovery retries on timeout/error."""
    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        # First attempt throws exception, second returns empty list
        mock_wsd.searchServices.side_effect = [Exception("Network error"), []]

        result = discover_onvif_cameras(timeout=0.1, max_retries=2)

        assert result == []
        assert mock_wsd.searchServices.call_count == 2


def test_discover_onvif_cameras_custom_onvif_port() -> None:
    """Test discovery extracts custom ONVIF port from XAddr."""
    mock_service = Mock()
    mock_service.getXAddrs.return_value = ["http://192.168.0.100:8080/onvif/device_service"]

    with patch("vector.runtime.camera_discovery.WSDiscovery") as mock_wsd_class:
        mock_wsd = Mock()
        mock_wsd_class.return_value = mock_wsd
        mock_wsd.searchServices.return_value = [mock_service]

        result = discover_onvif_cameras(timeout=0.1, max_retries=1)

        assert len(result) == 1
        assert result[0]["ip"] == "192.168.0.100"
        assert result[0]["onvif_port"] == 8080