#
# Copyright (c) 2026 Tripti Sharma
# Licensed under the MIT License
#
from typing import Any, Iterable, Mapping, Optional
import logging
import requests
from airbyte_cdk.sources.streams import Stream


class ProfilesStream(Stream):
    """
    Stream implementation for CleverTap Profiles API with 2-step cursor pagination
    """
    
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.account_id = config["account_id"]
        self.passcode = config["passcode"]
        self.event_name = config["event_name"]
        self.start_date = config["start_date"]
        self.end_date = config["end_date"]
        self.region = config.get("region", "")
        
        # Build base URL based on region
        if self.region:
            self.base_url = f"https://{self.region}.api.clevertap.com"
        else:
            self.base_url = "https://api.clevertap.com"
        
        # Track if we've fetched the initial cursor
        self._initial_cursor = None
        self._current_cursor = None
        self._logger = logging.getLogger(f"airbyte.{self.name}")
    
    @property
    def name(self) -> str:
        """Stream name"""
        return "profiles"
    
    @property
    def logger(self) -> logging.Logger:
        """Return logger"""
        return self._logger
    
    def request_headers(self) -> Mapping[str, Any]:
        """Return headers required for CleverTap API authentication"""
        return {
            "X-CleverTap-Account-Id": self.account_id,
            "X-CleverTap-Passcode": self.passcode,
            "Content-Type": "application/json"
        }
    
    def _get_initial_cursor(self) -> Optional[str]:
        """
        STEP 1: Make POST request to get initial cursor
        This request does NOT return records, only a cursor
        """
        url = f"{self.base_url}/1/profiles.json"
        headers = self.request_headers()
        
        payload = {
            "event_name": self.event_name,
            "from": self.start_date,
            "to": self.end_date
        }
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") != "success":
            raise Exception(f"API returned error status: {data}")
        
        cursor = data.get("cursor")
        if not cursor:
            raise Exception(f"No cursor returned from API: {data}")
        
        return cursor
    
    def read_records(
        self,
        sync_mode: str,
        cursor_field: list = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Override read_records to implement the 2-step cursor pagination
        
        STEP 1: POST to get initial cursor (done once)
        STEP 2: GET with cursor repeatedly until no more cursor in response
        """
        import time
        
        # Step 1: Get initial cursor if we haven't already
        if not self._initial_cursor:
            self.logger.info("Fetching initial cursor from CleverTap API...")
            self._initial_cursor = self._get_initial_cursor()
            self._current_cursor = self._initial_cursor
            if self._current_cursor:
                self.logger.info(f"Obtained initial cursor: {self._current_cursor[:50]}...")
        
        # Step 2: Loop through GET requests with cursor until exhausted
        page_count = 0
        total_records = 0
        
        while self._current_cursor:
            page_count += 1
            self.logger.info(f"Fetching page {page_count} with cursor: {self._current_cursor[:50]}...")
            
            # Retry logic for async API
            max_retries = 10
            retry_delay = 5  # seconds
            
            for attempt in range(max_retries):
                # Make POST request with cursor
                url = f"{self.base_url}/1/profiles.json"
                headers = self.request_headers()
                payload = {"cursor": self._current_cursor}
                
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                
                data = response.json()
                
                # Check if request is still in progress (code 2)
                if data.get("status") == "fail" and data.get("code") == 2:
                    if attempt < max_retries - 1:
                        self.logger.info(f"Request still in progress. Waiting {retry_delay} seconds before retry {attempt + 1}/{max_retries}...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise Exception(f"Max retries reached. API still not ready: {data}")
                
                # Check for other errors
                if data.get("status") != "success":
                    raise Exception(f"API returned error status: {data}")
                
                # Success! Break out of retry loop
                break
            
            # Extract records
            records = data.get("records", [])
            self.logger.info(f"Page {page_count}: Received {len(records)} records")
            
            # Yield each record
            for record in records:
                total_records += 1
                yield record
            
            # Check for next cursor
            next_cursor = data.get("cursor")
            
            if next_cursor:
                self._current_cursor = next_cursor
            else:
                # No more pages
                self.logger.info(f"No more pages. Total records fetched: {total_records}")
                self._current_cursor = None
                break
        
        if total_records == 0:
            self.logger.info("No records found for the given criteria")
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Return JSON schema for the stream
        """
        import os
        import json
        
        schema_path = os.path.join(
            os.path.dirname(__file__),
            "schemas",
            f"{self.name}.json"
        )
        
        with open(schema_path, "r") as f:
            return json.load(f)

class EventsStream(Stream):
    """
    Stream implementation for CleverTap Events API with 2-step cursor pagination
    """
    
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.account_id = config["account_id"]
        self.passcode = config["passcode"]
        self.event_name = config.get("event_name")  # Optional for events
        self.start_date = config["start_date"]
        self.end_date = config["end_date"]
        self.region = config.get("region", "")
        
        # Build base URL based on region
        if self.region:
            self.base_url = f"https://{self.region}.api.clevertap.com"
        else:
            self.base_url = "https://api.clevertap.com"
        
        # Track if we've fetched the initial cursor
        self._initial_cursor = None
        self._current_cursor = None
        self._logger = logging.getLogger(f"airbyte.{self.name}")
    
    @property
    def name(self) -> str:
        """Stream name"""
        return "events"
    
    @property
    def logger(self) -> logging.Logger:
        """Return logger"""
        return self._logger
    
    def request_headers(self) -> Mapping[str, Any]:
        """Return headers required for CleverTap API authentication"""
        return {
            "X-CleverTap-Account-Id": self.account_id,
            "X-CleverTap-Passcode": self.passcode,
            "Content-Type": "application/json"
        }
    
    def _get_initial_cursor(self) -> Optional[str]:
        """
        STEP 1: Make POST request to get initial cursor for events
        """
        url = f"{self.base_url}/1/events.json"  # Note: events endpoint
        headers = self.request_headers()
        
        payload = {
            "from": self.start_date,
            "to": self.end_date
        }
        
        # Add event_name filter if specified
        if self.event_name:
            payload["event_name"] = self.event_name
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") != "success":
            raise Exception(f"API returned error status: {data}")
        
        cursor = data.get("cursor")
        if not cursor:
            raise Exception(f"No cursor returned from API: {data}")
        
        return cursor
    
    def read_records(
        self,
        sync_mode: str,
        cursor_field: list = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Override read_records to implement the 2-step cursor pagination for events
        """
        import time
        
        # Step 1: Get initial cursor if we haven't already
        if not self._initial_cursor:
            self.logger.info("Fetching initial cursor from CleverTap Events API...")
            self._initial_cursor = self._get_initial_cursor()
            self._current_cursor = self._initial_cursor
            if self._current_cursor:
                self.logger.info(f"Obtained initial cursor: {self._current_cursor[:50]}...")
        
        # Step 2: Loop through POST requests with cursor until exhausted
        page_count = 0
        total_records = 0
        
        while self._current_cursor:
            page_count += 1
            self.logger.info(f"Fetching page {page_count} with cursor: {self._current_cursor[:50]}...")
            
            # Retry logic for async API
            max_retries = 10
            retry_delay = 5  # seconds
            
            for attempt in range(max_retries):
                # Make POST request with cursor
                url = f"{self.base_url}/1/events.json"  # Note: events endpoint
                headers = self.request_headers()
                payload = {"cursor": self._current_cursor}
                
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                
                data = response.json()
                
                # Check if request is still in progress (code 2)
                if data.get("status") == "fail" and data.get("code") == 2:
                    if attempt < max_retries - 1:
                        self.logger.info(f"Request still in progress. Waiting {retry_delay} seconds before retry {attempt + 1}/{max_retries}...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise Exception(f"Max retries reached. API still not ready: {data}")
                
                # Check for other errors
                if data.get("status") != "success":
                    raise Exception(f"API returned error status: {data}")
                
                # Success! Break out of retry loop
                break
            
            # Extract records
            records = data.get("records", [])
            self.logger.info(f"Page {page_count}: Received {len(records)} records")
            
            # Yield each record
            for record in records:
                total_records += 1
                yield record
            
            # Check for next cursor
            next_cursor = data.get("cursor")
            
            if next_cursor:
                self._current_cursor = next_cursor
            else:
                # No more pages
                self.logger.info(f"No more pages. Total records fetched: {total_records}")
                self._current_cursor = None
                break
        
        if total_records == 0:
            self.logger.info("No records found for the given criteria")
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Return JSON schema for the stream
        """
        import os
        import json
        
        schema_path = os.path.join(
            os.path.dirname(__file__),
            "schemas",
            f"{self.name}.json"
        )
        
        with open(schema_path, "r") as f:
            return json.load(f)