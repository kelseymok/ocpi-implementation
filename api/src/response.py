from enum import Enum
from typing import Dict, List, Union
from datetime import datetime, timezone


class ResponseStatusCode(str, Enum):
    generic_success = 1000
    generic_client_error = 2000
    invalid_missing_params = 2001
    not_enough_information = 2002
    unknown_location = 2003
    unknown_token = 2004
    generic_server_error = 3000
    unable_to_use_client_api = 3001
    unsupported_version = 3002
    not_matching_endpoints = 3003
    generic_hub_error = 4000
    unknown_receiver = 4001
    timeout_on_forwarded_request = 4002
    connection_problem = 4003


class ResponseBuilder:
    def __init__(self, data: Union[Dict, List], status_code: ResponseStatusCode, status_message: str = None):
        self.data = data
        self.status_code = status_code
        self.status_message = status_message

    def format(self):
        return {
            "data": self.data,
            "status_code": self.status_code,
            "status_message": self.status_message,
            "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        }



