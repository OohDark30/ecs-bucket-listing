"""
DELL EMC ECS API Data Collection Module.
"""
import logging
import os
import json
import numbers

# Constants
BASE_CONFIG = 'BASE'                                          # Base Configuration Section
ECS_CONNECTION_CONFIG = 'ECS_CONNECTION'                      # ECS Connection Configuration Section
ECS_API_POLLING_INTERVALS = 'ECS_API_POLLING_INTERVALS'       # ECS API Call Interval Configuration Section


class InvalidConfigurationException(Exception):
    pass


class ECSBucketListingConfiguration(object):
    def __init__(self, config, tempdir):

        if config is None:
            raise InvalidConfigurationException("No file path to the ECS Data Collection Module configuration provided")

        if not os.path.exists(config):
            raise InvalidConfigurationException("The ECS Data Collection Module configuration "
                                                "file path does not exist: " + config)

        if tempdir is None:
            raise InvalidConfigurationException("No path for temporary file storage provided")

        # Store temp file storage path to the configuration object
        self.tempfilepath = tempdir

        # Attempt to open configuration file
        try:
            with open(config, 'r') as f:
                parser = json.load(f)
        except Exception as e:
            raise InvalidConfigurationException("The following unexpected exception occurred in the "
                                                "ECS Data Collection Module attempting to parse "
                                                "the configuration file: " + e.message)

        # We parsed the configuration file now lets grab values
        self.ecsconnections = parser[ECS_CONNECTION_CONFIG]

        # Set logging level
        logging_level_raw = parser[BASE_CONFIG]['logging_level']
        self.namespace = parser[BASE_CONFIG]['namespace']
        self.objectuser = parser[BASE_CONFIG]['objectuser']
        self.logging_level = logging.getLevelName(logging_level_raw.upper())

        # Grab ECS API Polling Intervals
        self.modules_intervals = parser[ECS_API_POLLING_INTERVALS]

        # Validate logging level
        if logging_level_raw not in ['debug', 'info', 'warning', 'error']:
            raise InvalidConfigurationException(
                "Logging level can be only one of ['debug', 'info', 'warning', 'error']")

        # Iterate through ECS API Module Interval Configuration and make sure intervals are numeric greater than 0
        for i, j in self.modules_intervals.items():
            if not j.isnumeric():
                raise InvalidConfigurationException("The ECS API Polling Interval of " + j + " for API Call " + i +
                                                    " is not numeric.")

        # Iterate through all configured ECS connections and validate connection info
        for ecsconnection in self.ecsconnections:
            # Validate ECS Connections values
            if not ecsconnection['protocol']:
                raise InvalidConfigurationException("The ECS Management protocol is not "
                                                    "configured in the module configuration")
            if not ecsconnection['host']:
                raise InvalidConfigurationException("The ECS Management Host is not configured in the module configuration")
            if not ecsconnection['port']:
                raise InvalidConfigurationException("The ECS Management port is not configured in the module configuration")
            if not ecsconnection['user']:
                raise InvalidConfigurationException("The ECS Management User is not configured in the module configuration")
            if not ecsconnection['password']:
                raise InvalidConfigurationException("The ECS Management Users password is not configured "
                                                    "in the module configuration")
            # Validate API query parameters
            if not ecsconnection['dataType']:
                ecsconnection['dataType'] = "default"

            if not ecsconnection['category']:
                ecsconnection['category'] = "default"

            if not ecsconnection['connectTimeout']:
                ecsconnection['connectTimeout'] = "15"

            if not ecsconnection['readTimeout']:
                ecsconnection['readTimeout'] = "60"
