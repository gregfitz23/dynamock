package com.bizo.aws.dynamock.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.bizo.aws.dynamock.DynamockDBTable;
import com.bizo.aws.dynamock.DynamockDBTableManager;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * An implementation of DynamockDBTableManager that uses MongoDB for a persistent data store.
 * Tables are mapped to collections.  Hash and range keys are created as indexes.
 * @author gregfitzgerald
 *
 */
public class DynamockDBTableManagerMongoDBImpl  implements DynamockDBTableManager {

  private static final String SCHEMA_INFO_RANGE_KEY_NAME = "rangeKeyName";
  private static final String SCHEMA_INFO_HASH_KEY_NAME = "hashKeyName";
  private static final String SCHEMA_INFO_TABLE_NAME = "tableName";
  private Mongo connection;
  DB mongoDB;
  DBCollection schemaInfo;
  
  /**
   * Create a connection to the Mongo db using the default host and port.
   * @param dbName
   * @throws UnknownHostException
   * @throws MongoException
   */
  public DynamockDBTableManagerMongoDBImpl(final String dbName) throws UnknownHostException, MongoException {
    this("localhost", 27017, dbName);
  }
  
  /**
   * Create a connection to the mongo db and create or find the schema_info collection.
   * @param host
   * @param port
   * @param dbName
   * @throws UnknownHostException
   * @throws MongoException
   */
  public DynamockDBTableManagerMongoDBImpl(final String host, final int port, final String dbName) throws UnknownHostException, MongoException {
    connection = new Mongo(host, port);
    mongoDB = connection.getDB(dbName);
    
    final String schemaInfoName = "schema_info";
    findOrCreateCollection(schemaInfoName);
  }

  /**
   * Close the mongo connection
   */
  public void close() {
    connection.close();
  }
  
  /**
   * Lookup a table in the schema_info and return a DynamockDBTable representation of it.
   */
  @Override
  public DynamockDBTable getTable(final String tableName) {
    final DBObject schemaObj = getTableSchemaInfo(tableName);
    
    if (schemaObj == null) {
      return null;
    }
    
    final DBCollection collection = mongoDB.getCollection(tableName);
    final DynamockDBTable table = new DynamockDBTableMongoDBImpl(collection);
    table.setTableName((String)schemaObj.get(SCHEMA_INFO_TABLE_NAME));
    table.setHashKeyName((String)schemaObj.get(SCHEMA_INFO_HASH_KEY_NAME));
    table.setRangeKeyName((String)schemaObj.get(SCHEMA_INFO_RANGE_KEY_NAME));
    
    return table;
  }


  /**
   * Lookup all tables in schema_info and return a collection of DynamockDBTables
   */
  @Override
  public Collection<DynamockDBTable> getTables() {
    final Collection<String> tableNames = new ArrayList<String>();
    final DBCursor cursor = schemaInfo.find();
    final Iterator<DBObject> iterator = cursor.iterator();
    
    while (iterator.hasNext()) {
      final DBObject schemaObj = iterator.next();
      final String tableName = (String)schemaObj.get(SCHEMA_INFO_TABLE_NAME);
      tableNames.add(tableName);
    }
    
    final Collection<DynamockDBTable> tables = new ArrayList<DynamockDBTable>();
    for(String tableName : tableNames) {
      tables.add(getTable(tableName));
    }
    
    return tables;
  }

  /**
   * Creates a new mongo db collection and schema_info entry for that collection.  Returns a DynamoDBTable reflecting that collection.
   * @param tableName the name of the table
   * @param keySchema the schema reflecting hash and range keys
   */
  @Override
  public DynamockDBTable createTable(final String tableName, final KeySchema keySchema) {
    final DBCollection collection = mongoDB.createCollection(tableName, new BasicDBObject());

    final DynamockDBTable table = new DynamockDBTableMongoDBImpl(collection);
    final KeySchemaElement hashKey = keySchema.getHashKeyElement();
    final KeySchemaElement rangeKey = keySchema.getRangeKeyElement();
    
    final DBObject indexObj = new BasicDBObject();
    final DBObject schemaObj = new BasicDBObject();
    
    schemaObj.put(SCHEMA_INFO_TABLE_NAME, tableName);
    schemaObj.put(SCHEMA_INFO_HASH_KEY_NAME, hashKey.getAttributeName());
    indexObj.put(hashKey.getAttributeName(), 1);

    table.setTableName(tableName);
    table.setHashKeyName(hashKey.getAttributeName());

    if (rangeKey != null) {
      schemaObj.put(SCHEMA_INFO_RANGE_KEY_NAME, rangeKey.getAttributeName());
      indexObj.put(rangeKey.getAttributeName(), 1);
      table.setRangeKeyName(rangeKey.getAttributeName());
    }
    
    schemaInfo.insert(schemaObj);
    collection.createIndex(indexObj);
    
    return table;
  }

  /**
   * Drop the collection and remove the schema_info entry for tableName
   * @param tableName the tableName to delete
   */
  @Override
  public void deleteTable(final String tableName) {
    final DBObject schemaObj = getTableSchemaInfo(tableName);
    schemaObj.removeField("_id");
    schemaInfo.remove(schemaObj);
    mongoDB.getCollection(tableName).drop();
  }

  

  /**
   * Lookup a the schema info for tableName as a DBObject.
   * @param tableName
   * @return DBObject
   */
  private DBObject getTableSchemaInfo(final String tableName) {
    final DBObject query = new BasicDBObject();
    query.put(SCHEMA_INFO_TABLE_NAME, tableName);
    final DBObject schemaObj = schemaInfo.findOne(query);
    return schemaObj;
  }
  
  private void findOrCreateCollection(final String collectionName) {
    if (mongoDB.collectionExists(collectionName)) {
      schemaInfo = mongoDB.getCollection(collectionName);
    } else {
      schemaInfo = mongoDB.createCollection(collectionName, new BasicDBObject());
    }
  }
  
}
