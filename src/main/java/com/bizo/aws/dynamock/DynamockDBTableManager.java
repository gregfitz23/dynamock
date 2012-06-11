package com.bizo.aws.dynamock;

import java.util.Collection;

import com.amazonaws.services.dynamodb.model.KeySchema;

/**
 * Interface defining the contract of table CRUD
 * @author gregfitzgerald
 *
 */
public interface DynamockDBTableManager {
  public DynamockDBTable getTable(String tableName);
  public Collection<DynamockDBTable> getTables();
  public DynamockDBTable createTable(String tableName, KeySchema keySchema);
  public void deleteTable(String tableName);
}
