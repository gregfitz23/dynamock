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

  public abstract String getTableName();

  public abstract void setTableName(String tableName);

  public abstract String getHashKeyName();

  public abstract void setHashKeyName(String hashKeyName);

  public abstract String getRangeKeyName();

  public abstract void setRangeKeyName(String rangeKeyName);

  public abstract Map<AttributeValue, DynamockDBItem> getItemsForHashKey(
      final AttributeValue hashKey);

  public abstract List<DynamockDBItem> getAllItems();

  public abstract DynamockDBItem getItem(final Key key);

  public abstract void putItem(final Key key, final DynamockDBItem item);
  
  public abstract DynamockDBItem createItem(final Map<String, AttributeValue> item);

  public abstract void deleteItem(final Key key);

}