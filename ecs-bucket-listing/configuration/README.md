# ecs-bucket-listing configuration
----------------------------------------------------------------------------------------------
ecs-bucket-listing is a PYTHON based sample application for DELL EMC's Elastic Cloud Storage Product.

ecs-bucket-listing utilizes the ECS Management REST API's to gather all buckets from a namespace and
filter by object user

We've provided two sample configuration files:

- ecs_config.sample: Change file suffix from .sample to .json and configure as needed
  This contains the tool configuration for ECS connection, logging level, namespace and object user to 
  list buckets for, etc. Here is the sample configuration:
  
  BASE:
  logging_level - The default is "info" but it can be set to "debug" to generate a LOT of details
  namespace - This is the namespace to be used to query buckets for
  objectuser - This is the object user that we want to filter the list of buckets on
  
  ECS_CONNECTION:
  protocol - Should be set to "https"
  host - This is the IP address of FQDN of an ECS node
  port - This is always "4443" which is the ECS Management API port
  user - This is the user id of an ECS Management User 
  password - This is the password for the ECS Management User
  
  _**Note: The ECS_CONNECTION is a list of dictionaries so multiple sets of ECS connection data can 
        be configured to support polling multiple ECS Clusters**_
  
  
  ECS_API_POLLING_INTERVALS
  This is a dictionary that contains the names of the ECSManagementAPI class methods that are used to perform 
  data extraction along with a numeric value that defines the polling interval in seconds to be used to call the method.
  
  "ecs_collect_bucket_info()": "30", 
  
_**Note: This is a construct from another project and is intended to be used as a background process
        to run every X seconds.   Not really needed for this sample**_
  
  

