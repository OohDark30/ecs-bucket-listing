"""
DELL EMC ECS API Data Collection Module.
"""

from configuration.ecs_configuration import ECSBucketListingConfiguration
from logger import ecs_logger
from ecs.ecs import ECSAuthentication
from ecs.ecs import ECSManagementAPI
import datetime
import os
import traceback
import signal
import time
import logging
import threading
import xml.etree.ElementTree as ET

# Constants
MODULE_NAME = "ECS_Data_Collection_Module"                  # Module Name
INTERVAL = 30                                               # In seconds
CONFIG_FILE = 'ecs_config.json'                             # Default Configuration File

# Globals
_configuration = None
_ecsManagementNode = None
_ecsManagementUser = None
_ecsManagementUserPassword = None
_logger = None
_ecsAuthentication = list()
_ecsManagmentAPI = {}

"""
Class to listen for signal termination for controlled shutdown
"""


class ECSDataCollectionShutdown:

    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.controlled_shutdown)
        signal.signal(signal.SIGTERM, self.controlled_shutdown)

    def controlled_shutdown(self, signum, frame):
        self.kill_now = True


class ECSDataCollection (threading.Thread):
    def __init__(self, method, logger, ecsmanagmentapi, pollinginterval, tempdir):
        threading.Thread.__init__(self)
        self.method = method
        self.logger = logger
        self.ecsmanagmentapi = ecsmanagmentapi
        self.pollinginterval = pollinginterval
        self.tempdir = tempdir

        logger.info(MODULE_NAME + '::ECSDataCollection()::init method of class called')

    def run(self):
        try:
            self.logger.info(MODULE_NAME + '::ECSDataCollection()::Starting thread with method: ' + self.method)

            if self.method == 'ecs_collect_bucket_info()':
                ecs_collect_bucket_info(self.logger, self.ecsmanagmentapi, self.pollinginterval, self.tempdir)
            else:
                self.logger.info(MODULE_NAME + '::ECSDataCollection()::Requested method ' +
                                 self.method + ' is not supported.')
        except Exception as e:
            _logger.error(MODULE_NAME + 'ECSDataCollection::run()::The following unexpected '
                                        'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_config(config, temp_dir):
    global _configuration
    global _logger
    global _ecsAuthentication

    try:
        # Load and validate module configuration
        _configuration = ECSBucketListingConfiguration(config, temp_dir)

        # Grab loggers and log status
        _logger = ecs_logger.get_logger(__name__, _configuration.logging_level)
        _logger.info(MODULE_NAME + '::ecs_config()::We have configured logging level to: '
                     + logging.getLevelName(str(_configuration.logging_level)))
        _logger.info(MODULE_NAME + '::ecs_config()::Configuring ECS Data Collection Module complete.')
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_config()::The following unexpected '
                                    'exception occured: ' + str(e) + "\n" + traceback.format_exc())


def ecs_collect_bucket_info(logger, ecsmanagmentapi, pollinginterval, tempdir):
    global _configuration

    try:
        # Start polling loop
        while True:
            # Perform API call against each configured ECS
            for key in ecsmanagmentapi:

                # Grab object
                ecsconnection = ecsmanagmentapi[key]

                # Reset marker
                next_marker = None

                # Reset buckets counter
                new_buckets = 0

                while True:
                    # Retrieve current bucket data via API for current VDC.  This may be
                    # called multiple times to iterate thru all buckets depending on
                    # of buckets i.e. deal with default page size of 1000
                    bucket_data_file = ecsconnection.ecs_get_bucket_data(tempdir, next_marker,
                                                                         _configuration.namespace)

                    if bucket_data_file is None:
                        logger.info(MODULE_NAME + '::ecs_collect_bucket_info()::'
                                                  'Unable to retrieve ECS Bucket Information')
                        return
                    else:
                        """
                        We have an XML File lets parse it
                        """
                        try:
                            tree = ET.parse(bucket_data_file)
                            root = tree.getroot()

                            # Grab next marker information
                            nm = root.find('NextMarker')
                            if nm is None:
                                next_marker = None
                            else:
                                next_marker = nm.text

                            # For each bucket grab bucket id and owner and add it to counter
                            for bucket in root.findall('object_bucket'):
                                bucketid = bucket.find('id').text
                                owner = bucket.find('owner').text

                                # If we are filtering on a specific object user only add it to
                                # the counter if we have a match
                                if not _configuration.objectuser:
                                    new_buckets += 1
                                else:
                                    if owner == _configuration.objectuser:
                                        new_buckets += 1

                            # No need to close the file as the ET parse()
                            # method will close it when parsing is completed.

                            _logger.debug(MODULE_NAME + '::ecs_collect_bucket_info::Deleting temporary '
                                                        'xml file: ' + bucket_data_file)

                        except Exception as ex:
                            logger.error(MODULE_NAME + '::ecs_collect_bucket_info()::The following unexpected '
                                                       'exception occurred: ' + str(ex) + "\n" + traceback.format_exc())

                    # Check to see if the marker is empty
                    if next_marker is None:
                        break

                # Log stats line
                if not _configuration.objectuser:
                    _logger.info(MODULE_NAME + '::ecs_collect_bucket_info::Discovered ' + str(new_buckets) +
                             ' buckets for namespace ' + _configuration.namespace)
                else:
                    _logger.info(MODULE_NAME + '::ecs_collect_bucket_info::Discovered ' + str(new_buckets) +
                         ' buckets for namespace ' + _configuration.namespace + ' and object user ' + _configuration.objectuser)

            if controlledShutdown.kill_now:
                logger.info(MODULE_NAME + '::ecs_collect_bucket_info()::Shutdown detected.  Terminating polling.')
                break

            # Wait for specific polling interval
            time.sleep(float(pollinginterval))
    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_collect_bucket_info()::The following unexpected '
                                    'exception occurred: ' + str(e) + "\n" + traceback.format_exc())


def ecs_authenticate():
    global _ecsAuthentication
    global _configuration
    global _logger
    global _ecsManagmentAPI
    connected = True

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Iterate over all ECS Connections configured and attempt tp Authenticate to ECS
        for ecsconnection in _configuration.ecsconnections:

            # Attempt to authenticate
            auth = ECSAuthentication(ecsconnection['protocol'], ecsconnection['host'], ecsconnection['user'],
                                     ecsconnection['password'], ecsconnection['port'], _logger)

            auth.connect()

            # Check to see if we have a token returned
            if auth.token is None:
                _logger.error(MODULE_NAME + '::ecs_init()::Unable to authenticate to ECS as configured.  '
                             'Please validate and try again.')
                connected = False
                break
            else:
                _ecsAuthentication.append(auth)

                # Instantiate ECS Management API object, and it to our list, and validate that we are authenticated
                _ecsManagmentAPI[ecsconnection['host']] = ECSManagementAPI(auth, ecsconnection['connectTimeout'],
                                                                            ecsconnection['readTimeout'],_logger)
                if not _ecsAuthentication:
                    _logger.info(MODULE_NAME + '::ecs_authenticate()::ECS Data Collection '
                                               'Module is not ready.  Please check logs.')
                    connected = False
                    break

        return connected

    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_init()::Cannot initialize plugin. Cause: '
                      + str(e) + "\n" + traceback.format_exc())
        connected = False


def ecs_data_collection():
    global _ecsAuthentication
    global _logger
    global _ecsManagmentAPI

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Now lets spin up a thread for each API call with it's own custom polling interval by iterating
        # through our module configuration
        for i, j in _configuration.modules_intervals.items():
            method = str(i)
            interval = str(j)
            t = ECSDataCollection(method, _logger, _ecsManagmentAPI, interval, _configuration.tempfilepath)
            t.start()

    except Exception as e:
        _logger.error(MODULE_NAME + '::ecs_data_collection()::A failure ocurred during data collection. Cause: '
                      + str(e) + "\n" + traceback.format_exc())


"""
Main 
"""
if __name__ == "__main__":

    try:
        # Create object to support controlled shutdown
        controlledShutdown = ECSDataCollectionShutdown()

        # Dump out application path
        currentApplicationDirectory = os.getcwd()
        configFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "configuration", CONFIG_FILE))
        tempFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "temp"))

        # Create temp diretory if it doesn't already exists
        if not os.path.isdir(tempFilePath):
            os.mkdir(tempFilePath)
        else:
            # The directory exists so lets scrub any temp XML files out that may be in there
            files = os.listdir(tempFilePath)
            for file in files:
                if file.endswith(".xml"):
                    os.remove(os.path.join(currentApplicationDirectory, "temp", file))

        # Initialize configuration and VDC Lookup
        ecs_config(configFilePath, tempFilePath)

        # Wait till we have a valid configuration object
        while not _configuration:
            time.sleep(1)

        _logger.info(MODULE_NAME + '::ecs_bucket_listing:main()::Configuration initialization complete.')
        _logger.info(MODULE_NAME + '::ecs_bucket_listing:main()::Current directory is : ' + currentApplicationDirectory)
        _logger.info(MODULE_NAME + '::ecs_bucket_listing:main()::Temp directory is : ' + tempFilePath)

        # Initialize connection(s) to ECS
        if ecs_authenticate():

            # Launch ECS Data Collection polling threads
            ecs_data_collection()

            # Check for shutdown
            if controlledShutdown.kill_now:
                print(MODULE_NAME + "__main__::Controlled shutdown completed.")

    except Exception as e:
        print(MODULE_NAME + '__main__::The following unexpected error occured: '
              + str(e) + "\n" + traceback.format_exc())

