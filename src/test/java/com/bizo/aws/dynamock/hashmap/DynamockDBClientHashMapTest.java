package com.bizo.aws.dynamock.hashmap;

import com.bizo.aws.dynamock.DynamockDBClient;
import com.bizo.aws.dynamock.DynamockDBClientTest;

public class DynamockDBClientHashMapTest extends DynamockDBClientTest {

  @Override
  protected void initializeDB() {
    db = new DynamockDBClient(new DynamockDBTableManagerHashMapImpl());
  }

}
