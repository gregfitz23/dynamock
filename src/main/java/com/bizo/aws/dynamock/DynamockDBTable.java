package com.bizo.aws.dynamock;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;

/**
 * An interface representing DynamockDB tables.  
 * Tables provide hash key, (optional) range key, and item accessor methods
 * @author gregfitzgerald
 *
 */
public interface DynamockDBTable {

  public String getTableName();

  public void setTableName(String tableName);

  public String getHashKeyName();

  public void setHashKeyName(String hashKeyName);

  public String getRangeKeyName();

  public void setRangeKeyName(String rangeKeyName);

  public Map<AttributeValue, DynamockDBItem> getItemsForHashKey(
      AttributeValue hashKey);

  public List<DynamockDBItem> getAllItems();

  public DynamockDBItem getItem(Key key);

  public void putItem(Key key, DynamockDBItem item);
  
  public DynamockDBItem createItem(Map<String, AttributeValue> item);

  public void deleteItem(Key key);

}