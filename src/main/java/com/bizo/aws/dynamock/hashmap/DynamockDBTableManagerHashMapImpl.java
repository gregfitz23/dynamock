package com.bizo.aws.dynamock.hashmap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.bizo.aws.dynamock.DynamockDBTable;
import com.bizo.aws.dynamock.DynamockDBTableManager;

/**
 * An implementation of DynamockDBTableManager utilizing HashMap for table storage
 * @author gregfitzgerald
 *
 */
public class DynamockDBTableManagerHashMapImpl implements DynamockDBTableManager {
  private final Map<String, DynamockDBTable> tables = new HashMap<String, DynamockDBTable>();
  
  @Override
  public DynamockDBTable getTable(String tableName) {
    return tables.get(tableName);
  }
  
  @Override
  public Collection<DynamockDBTable> getTables() {
    return tables.values();
  }

  @Override
  public DynamockDBTable createTable(String tableName, KeySchema keySchema) {
    final KeySchemaElement hashKey = keySchema.getHashKeyElement();
    final KeySchemaElement rangeKey = keySchema.getRangeKeyElement();

    final DynamockDBTable table = new DynamockDBTableHashMapImpl();
    table.setTableName(tableName);
    table.setHashKeyName(hashKey.getAttributeName());
    if (rangeKey != null) {
      table.setRangeKeyName(rangeKey.getAttributeName());
    }
    
    // store the table
    tables.put(tableName, table);
    return table;
  }

  @Override
  public void deleteTable(String tableName) {
    tables.remove(tableName);
  }

}
