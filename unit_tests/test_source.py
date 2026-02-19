#
# Copyright (c) 2026 Tripti Sharma
# Licensed under the MIT License
#
import pytest
from unittest.mock import Mock, patch
from source_clevertap import SourceClevertap
from source_clevertap.streams import ProfilesStream, EventsStream


class TestSourceClevertap:
    """Test suite for SourceClevertap"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.config = {
            "account_id": "TEST-ACCOUNT-ID",
            "passcode": "TEST-PASSCODE",
            "region": "in1",
            "event_name": "Test Event",
            "start_date": 20220101,
            "end_date": 20220201
        }
    
    def test_check_connection_success(self):
        """Test successful connection check"""
        source = SourceClevertap()
        logger = Mock()
        
        with patch.object(ProfilesStream, '_get_initial_cursor', return_value="test_cursor"):
            success, error = source.check_connection(logger, self.config)
            assert success is True
            assert error is None
    
    def test_check_connection_missing_fields(self):
        """Test connection check with missing required fields"""
        source = SourceClevertap()
        logger = Mock()
        incomplete_config = {"account_id": "TEST"}
        
        success, error = source.check_connection(logger, incomplete_config)
        assert success is False
        assert "Missing required config field" in error
    
    def test_check_connection_invalid_date_range(self):
        """Test connection check with invalid date range"""
        source = SourceClevertap()
        logger = Mock()
        invalid_config = self.config.copy()
        invalid_config["start_date"] = 20220201
        invalid_config["end_date"] = 20220101  # End before start
        
        success, error = source.check_connection(logger, invalid_config)
        assert success is False
        assert "start_date must be less than or equal to end_date" in error
    
    def test_streams(self):
        """Test that source returns correct streams"""
        source = SourceClevertap()
        streams = source.streams(self.config)
        
        assert len(streams) == 2
        assert isinstance(streams[0], ProfilesStream)
        assert isinstance(streams[1], EventsStream)
        assert streams[0].name == "profiles"
        assert streams[1].name == "events"
    
    def test_profiles_stream_initialization(self):
        """Test ProfilesStream initializes correctly"""
        stream = ProfilesStream(self.config)
        
        assert stream.account_id == "TEST-ACCOUNT-ID"
        assert stream.passcode == "TEST-PASSCODE"
        assert stream.region == "in1"
        assert stream.event_name == "Test Event"
        assert stream.base_url == "https://in1.api.clevertap.com"
    
    def test_events_stream_initialization(self):
        """Test EventsStream initializes correctly"""
        stream = EventsStream(self.config)
        
        assert stream.account_id == "TEST-ACCOUNT-ID"
        assert stream.passcode == "TEST-PASSCODE"
        assert stream.base_url == "https://in1.api.clevertap.com"
    
    def test_region_url_mapping(self):
        """Test that region codes map to correct URLs"""
        regions = {
            "in1": "https://in1.api.clevertap.com",
            "us1": "https://us1.api.clevertap.com",
            "sg1": "https://sg1.api.clevertap.com",
            "eu1": "https://eu1.api.clevertap.com",
            "": "https://api.clevertap.com"
        }
        
        for region, expected_url in regions.items():
            config = self.config.copy()
            config["region"] = region
            stream = ProfilesStream(config)
            assert stream.base_url == expected_url