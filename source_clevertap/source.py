#
# Copyright (c) 2026 Tripti Sharma
# Licensed under the MIT License
#
from typing import Any, List, Mapping, Tuple
import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
#from .streams import ProfilesStream, EventsStream  
from .streams import EventsStream  

class SourceClevertap(AbstractSource):
    """
    Source implementation for CleverTap Profiles API
    """

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        """
        Test connection to CleverTap API by making a POST request to get a cursor.
        
        :param logger: Airbyte logger
        :param config: Configuration from config.json
        :return: (True, None) on success, (False, error_message) on failure
        """
        try:
            from datetime import datetime
            
            required_fields = ["account_id", "passcode", "start_date"]
            for field in required_fields:
                if field not in config:
                    return False, f"Missing required config field: {field}"
            
            start_date = config["start_date"]
            end_date = config.get("end_date") or int(datetime.now().strftime("%Y%m%d"))
            
            if not isinstance(start_date, int):
                return False, "start_date must be an integer in YYYYMMDD format"
            
            if isinstance(end_date, int) and start_date > end_date:
                return False, "start_date must be less than or equal to end_date"
            
            
            stream = EventsStream(config)
            cursor = stream._get_initial_cursor()
            
            if not cursor:
                return False, "Failed to get cursor from CleverTap API"
            
            logger.info(f"Successfully connected to CleverTap API and obtained cursor")
            return True, None
            
        except Exception as e:
            return False, f"Connection check failed: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Return list of streams for this source
        
        :param config: Configuration from config.json
        :return: List of streams
        """
        return [
            #ProfilesStream(config),
            EventsStream(config)  
        ]