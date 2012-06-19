Dynamock
========

Overview
--------
An Amazon DynamoDB drop-in replacement for in-memory testing and local data storage.


Usage
-------
Simply add the dynamock jar to your project and use an instance of com.bizo.aws.dynamock.DynamockDBClient for your com.amazonaws.services.dynamodb.AmazonDynamoDB implementation.

    AmazonDynamoDB client = new DynamockDBClient()

Dynamock is currently built against [aws-java-sdk](https://github.com/amazonwebservices/aws-sdk-for-java) version 1.3.10

DynamockDBTable data storage implementations
--------------
There are two storage options:

*DynamockDBTableHashMapImpl* - the default, in memory, non-persistent data storage.  Very fast, perfect for testing.  Tables are modeled as HashMaps.

    AmazonDynamoDB client = new DynamockDBClient()
    // is equivalent to
    AmazonDynamoDB client = new DynamockDBClient(new DynamockDBTableManagerHashMapImpl())

*DynmamockDBTableMongoDBImpl* - a MongoDB backed data store. Good for persisted local development.  Tables are modeled as Mongo collections.  AttributeValues are serialized as native POJOs.

    // default host and port
    AmazonDynamoDB client = new DynamockDBClient(new DynamockDBTableManagerMongoDBImpl("my-database-name"))
    // custom host and port
    AmazonDynamoDB client = new DynamockDBClient(new DynamockDBTableManagerMongoDBImpl("localhost", 12345, "my-database-name"))

Unsupported operations
-------------------
The following operations have not yet been implemented and will throw an UnsupportedOperationException if used.  They may be implemented in future versions:
* setEndpoint
* updateTable
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

Acknowledgements
-------------
This library was developed in conjunction with [Bizo, Inc.](http://www.bizo.com).