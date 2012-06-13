package com.bizo.aws.dynamock.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.bizo.aws.dynamock.DynamockDBItem;
import com.bizo.aws.dynamock.DynamockDBTable;
import com.bizo.aws.dynamock.hashmap.DynamockDBItemHashMapImpl;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * An implementation of DynamockDBTable with uses MongoDB as a data store. 
 * Handles the serialization to and from DynamoDBItem's
 * @author gregfitzgerald
 *
 */
public class DynamockDBTableMongoDBImpl implements DynamockDBTable {
  
  private DBCollection collection;
  private String tableName;
  private String hashKeyName;
  private String rangeKeyName;
  
  /**
   * Create a DynamockDBTable instance mapped to the underlying collection.
   * The collection must already exist.
   * @param collection
   */
  public DynamockDBTableMongoDBImpl(DBCollection collection) {
    this.collection = collection;
    collection.setObjectClass(AttributeValueDBObject.class);
  }
  
  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public String getHashKeyName() {
    return hashKeyName;
  }

  @Override
  public void setHashKeyName(String hashKeyName) {
    this.hashKeyName = hashKeyName;
  }

  @Override
  public String getRangeKeyName() {
    return rangeKeyName;
  }

  @Override
  public void setRangeKeyName(String rangeKeyName) {
    this.rangeKeyName = rangeKeyName;
  }

  @Override
  public Map<AttributeValue, DynamockDBItem> getItemsForHashKey(final AttributeValue hashKey) {
    final Key key = new Key();
    key.setHashKeyElement(hashKey);
    
    final AttributeValueDBObject query = queryObjForKey(key);

    final Map<AttributeValue, DynamockDBItem> items = new HashMap<AttributeValue, DynamockDBItem>();
    final DBCursor cursor = collection.find(query);
    final Iterator<DBObject> iterator = cursor.iterator();
    while (iterator.hasNext()) {
      final AttributeValueDBObject item = (AttributeValueDBObject)iterator.next();
      final AttributeValue rangeKey = (AttributeValue)item.get(rangeKeyName);
      if (rangeKey != null) {
        items.put(rangeKey, dynamockItemForDBObject(item));
      }
    }
    
    return items;
  }

  @Override
  public List<DynamockDBItem> getAllItems() {
    final List<DynamockDBItem> items = new ArrayList<DynamockDBItem>();
    final DBCursor cursor = collection.find();
    final Iterator<DBObject> iterator = cursor.iterator();
    
    while(iterator.hasNext()) {
      final AttributeValueDBObject dbObject = (AttributeValueDBObject)iterator.next();
      items.add(dynamockItemForDBObject(dbObject));
    }
    
    return items;
  }

  @Override
  public DynamockDBItem getItem(final Key key) {
    final AttributeValueDBObject query = queryObjForKey(key);
    
    final AttributeValueDBObject found = (AttributeValueDBObject)collection.findOne(query);
    
    if (found == null) { 
      return null;
    }
    
    return dynamockItemForDBObject(found);
  }

  @Override
  public void putItem(final Key key, final DynamockDBItem item) {
    final DBObject obj = new AttributeValueDBObject(item.toMap());
    final DynamockDBItem foundItem = getItem(key);
    
    if (foundItem == null) {
      collection.insert(obj);
    } else {
      collection.update(queryObjForKey(key), obj);
    }
  }

  @Override
  public DynamockDBItem createItem(Map<String, AttributeValue> item) {
    final DBObject obj = new AttributeValueDBObject(item);
    collection.insert(obj);

    return new DynamockDBItemHashMapImpl(item);
  }

  @Override
  public void deleteItem(final Key key) {
    AttributeValueDBObject query = queryObjForKey(key);
    collection.remove(query);
  }
  
  /**
   * Generate a DBObject for querying from the given Dynamo Key
   * @param key
   * @return
   */
  private AttributeValueDBObject queryObjForKey(final Key key) {
    final AttributeValueDBObject query = new AttributeValueDBObject();
    final AttributeValue rangeKey = key.getRangeKeyElement();
    query.put(hashKeyName, key.getHashKeyElement());
    if (rangeKey != null) {
      query.put(rangeKeyName, key.getRangeKeyElement());
    }
    return query;
  }
  
  /**
   * Generate a DyamockDBItem from the given DBObject
   * @param dbObject
   * @return
   */
  private DynamockDBItem dynamockItemForDBObject(
      final AttributeValueDBObject dbObject) {
    
    final Map<String, AttributeValue> attributeMap = new HashMap<String, AttributeValue>();
    for(String objKey: dbObject.keySet()) {
      Object val = dbObject.get(objKey);
      if (val instanceof AttributeValue) {
        attributeMap.put(objKey, (AttributeValue)val);
      }
    }
    
    return new DynamockDBItemHashMapImpl(attributeMap);
  }

}
