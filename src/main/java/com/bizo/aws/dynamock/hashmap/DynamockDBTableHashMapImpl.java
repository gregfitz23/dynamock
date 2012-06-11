package com.bizo.aws.dynamock.hashmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.ResourceNotFoundException;
import com.bizo.aws.dynamock.DynamockDBItem;
import com.bizo.aws.dynamock.DynamockDBTable;

/**
 * An implementation of DynamockDBTable utilizing HashMaps as the underlying table storage.
 * @author gregfitzgerald
 *
 */
public class DynamockDBTableHashMapImpl implements DynamockDBTable {
  private AttributeValue PLACEHOLDER = new AttributeValue().withS("placeholder");
  private String tableName;
  private String hashKeyName;
  private String rangeKeyName;
  
  private Map<AttributeValue, Map<AttributeValue, DynamockDBItem>> items = new HashMap<AttributeValue, Map<AttributeValue, DynamockDBItem>>();

  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#getTableName()
   */
  @Override
  public String getTableName() {
    return tableName;
  }

  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#setTableName(java.lang.String)
   */
  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#getHashKeyName()
   */
  @Override
  public String getHashKeyName() {
    return hashKeyName;
  }

  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#setHashKeyName(java.lang.String)
   */
  @Override
  public void setHashKeyName(String hashKeyName) {
    this.hashKeyName = hashKeyName;
  }

  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#getRangeKeyName()
   */
  @Override
  public String getRangeKeyName() {
    return rangeKeyName;
  }

  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#setRangeKeyName(java.lang.String)
   */
  @Override
  public void setRangeKeyName(String rangeKeyName) {
    this.rangeKeyName = rangeKeyName;
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#getItemsForHashKey(com.amazonaws.services.dynamodb.model.AttributeValue)
   */
  @Override
  public Map<AttributeValue, DynamockDBItem> getItemsForHashKey(final AttributeValue hashKey) {
    return items.get(hashKey);
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#getAllItems()
   */
  @Override
  public List<DynamockDBItem> getAllItems() {
    final List<DynamockDBItem> retList = new ArrayList<DynamockDBItem>();
    for (Map<AttributeValue, DynamockDBItem> itemsByRanges : items.values()) {
      retList.addAll(itemsByRanges.values());
    }
    
    return retList;
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#getItem(com.amazonaws.services.dynamodb.model.Key)
   */
  @Override
  public DynamockDBItem getItem(final Key key) {
    final AttributeValue hashKey = key.getHashKeyElement();
    final AttributeValue rangeKey = key.getRangeKeyElement();
    
    return getItem(hashKey, rangeKey);
  }
  
  /**
   * Lookup an item by hashkey and rangekey.  If range key is null, use a placeholder.
   * @param hashKey
   * @param rangeKey
   * @return
   */
  public DynamockDBItem getItem(final AttributeValue hashKey, final AttributeValue rangeKey) {
    AttributeValue lookupRangeKey = rangeKey;
    if (lookupRangeKey == null)
      lookupRangeKey = PLACEHOLDER;
    
    final Map<AttributeValue, DynamockDBItem> itemsByRangeKey = items.get(hashKey);
    if (itemsByRangeKey == null) {
      return null;
    } else{
      return itemsByRangeKey.get(lookupRangeKey);
    }
  }
  
  /**
   * Create an item, storing it in the database and returning a DynomockDBItem instance.
   */
  @Override
  public DynamockDBItem createItem(final Map<String, AttributeValue> item) {
    final DynamockDBItem itemObj = new DynamockDBItemHashMapImpl(item);
    final Key key = new Key();
    final AttributeValue hashKey = item.get(getHashKeyName());
    key.setHashKeyElement(hashKey);
    
    if (item.containsKey(getRangeKeyName())) {
      final AttributeValue rangeKey = item.get(rangeKeyName);
      key.setRangeKeyElement(rangeKey);
    }
    
    putItem(key, itemObj);
    
    return itemObj;
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#putItem(com.amazonaws.services.dynamodb.model.Key, com.bizo.comscore.aws.DynomockDBClient.Item)
   */
  /**
   * Put the given item into the items map, keyed by hashKey and rangeKey.  If rangeKey is null, use a defaul range key
   * @param hashKey
   * @param rangeKey
   * @param item
   */
  @Override
  public void putItem(final Key key, final DynamockDBItem item) {
    final AttributeValue hashKey = key.getHashKeyElement();
    final AttributeValue rangeKey = key.getRangeKeyElement();
    
    final DynamockDBItem itemObj = new DynamockDBItemHashMapImpl(item.toMap());
    final AttributeValue lookupRangeKey = rangeKeyOrDefault(rangeKey);
    
    Map<AttributeValue, DynamockDBItem> rangeKeyMap = items.get(hashKey);
    if (rangeKeyMap == null) {
      rangeKeyMap = new HashMap<AttributeValue, DynamockDBItem>();
      items.put(hashKey, rangeKeyMap);
    }
    
    rangeKeyMap.put(lookupRangeKey, itemObj);
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBTable#deleteItem(com.amazonaws.services.dynamodb.model.Key)
   */
  @Override
  public void deleteItem(final Key key) {
    deleteItem(key.getHashKeyElement(), key.getRangeKeyElement());
  }
  
  /**
   * Delete an item from the table by removing it from the hash.
   * @param hashKey
   * @param rangeKey
   */
  public void deleteItem(final AttributeValue hashKey, final AttributeValue rangeKey) {
    final Map<AttributeValue, DynamockDBItem> rangeKeyMap = items.get(hashKey);
    if (rangeKeyMap == null) {
      throw new ResourceNotFoundException("Item not found");
    }
    
    rangeKeyMap.remove(rangeKeyOrDefault(rangeKey));
  }
  
  private AttributeValue rangeKeyOrDefault(final AttributeValue rangeKey) {
    return (rangeKey == null) ? PLACEHOLDER : rangeKey;
  }
}
