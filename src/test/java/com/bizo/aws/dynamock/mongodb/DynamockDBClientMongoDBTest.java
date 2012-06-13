package com.bizo.aws.dynamock.mongodb;

import java.net.UnknownHostException;
import java.util.Collection;

import org.junit.After;

import com.bizo.aws.dynamock.DynamockDBClient;
import com.bizo.aws.dynamock.DynamockDBClientTest;
import com.bizo.aws.dynamock.DynamockDBTable;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;

public class DynamockDBClientMongoDBTest extends DynamockDBClientTest {

  private DynamockDBTableManagerMongoDBImpl tableManager; 
  
  
  @Override
  protected void initializeDB() {
    try {
      tableManager = new DynamockDBTableManagerMongoDBImpl("mongo-test");
      db = new DynamockDBClient(tableManager);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    } catch (MongoException e) {
      throw new RuntimeException(e);
    }
  }
  
  @After
  public void shutdown() {
    cleanupTables();
    tableManager.close();
  }

  private void cleanupTables() {
    final Collection<String> tableNames = tableManager.mongoDB.getCollectionNames();
    for(String tableName: tableNames) {
      if (!tableName.equals("system.indexes")) {
        final DBCollection coll = tableManager.mongoDB.getCollection(tableName);
        coll.drop();
      }
    }
  }
}
