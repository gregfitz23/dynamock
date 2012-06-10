dynamock
========

Overview
--------
An Amazon DynamoDB drop in replacement for running tests locally and in-memory.

Usage
-------
Simply add the dynamock jar to your project and use an instance of com.bizo.aws.dynamock.DynamockDBClient for your com.amazonaws.services.dynamodb.AmazonDynamoDB implementation.
    AmazonDynamoDB client = new DynamockDBClient()
Dynamock is currently built against [aws-java-sdk](https://github.com/amazonwebservices/aws-sdk-for-java) version 1.3.10

DynamockDBTable data storage implementations
--------------
Currently all data storage is in memory and non-persistent.  This is perfect for testing, less so for local development.  Future versions will include a mechanism for persisting data locally.  All implementations must implement the DynamockDBTable interface.
*DynamockDBTableHashMapImpl* - the default, in memory, non-persistent data storage.
*DynmamockDBTableRedisImpl (planned)* - a Redis backed data store


Unsupported operations
-------------------
The following operations have not yet been implemented and will throw an UnsupportedOperationException if used.  They may be implemented in future versions:
* setEndpoint
* updateTable
* batchWriteItem
* describeTable
* getCachedResponseMetadata
* shutdown
  
Contributing
-------------
1. Fork the repo
2. Create a test for your change
3. Implement your change
4. Ensure all tests pass
5. Push your repo and submit a pull request