package com.bizo.aws.dynamock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteResponse;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.PutRequest;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScalarAttributeType;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.WriteRequest;

public abstract class DynamockDBClientTest {

  protected AmazonDynamoDB db;
  private final String hashKeyName = "test hash key";
  private final String rangeKeyName = "test range key";
  
  private final String hashKeyOnlyTableName = "test table name hash key only";
  private final String hashAndRangeTableName = "test table hash and range";
  
  private final Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

  //item
  private final String itemHashKeyValue = "hash key value";
  private final String itemRangeKeyValue = "range key value";
  
  private final String itemStringAttributeName = "other string attribute";
  private final String itemStringAttributeValue = "other attribute value";
  
  private final String itemNumberAttributeName = "other number attribute";
  private final String itemNumberAttributeValue = "2";
  
  private final String itemStringSetAttributeName = "other string set attribute";
  private final Set<String> itemStringSetAttributeValue = new HashSet<String>();
  private final String itemStringSetElementAttributeValue = "string set attribute";
  
  private final String itemNumberSetAttributeName = "other number set attribute";
  private final Set<String> itemNumberSetAttributValue = new HashSet<String>();
  private final String itemNumberSetElementAttributeValue = "5";
  
  @Before
  public void setup() {
    initializeDB();
    
    createTable(hashKeyOnlyTableName, hashKeyName);
    createTable(hashAndRangeTableName, hashKeyName, rangeKeyName);
    
    itemStringSetAttributeValue.add(itemStringSetElementAttributeValue);
    itemNumberSetAttributValue.add(itemNumberSetElementAttributeValue);
    item.put(hashKeyName, new AttributeValue().withS(itemHashKeyValue));
    item.put(itemStringAttributeName, new AttributeValue().withS(itemStringAttributeValue));
    item.put(itemNumberAttributeName, new AttributeValue().withN(itemNumberAttributeValue));
    item.put(itemStringSetAttributeName, new AttributeValue().withSS(itemStringSetAttributeValue));
    item.put(itemNumberSetAttributeName, new AttributeValue().withNS(itemNumberSetAttributValue));
  }
  
  protected abstract void initializeDB();
  
  /**
   * Test that create table returns a valid result.
   */
  @Test
  public void testCreateTable() {
    final String tableName = "tableName";
    final String hashKeyName = "hashKey";
    final CreateTableResult result = createTable(tableName, hashKeyName);
    
    final TableDescription tableDescription = result.getTableDescription();

    final KeySchemaElement hashKey = new KeySchemaElement()
    .withAttributeName(hashKeyName)
    .withAttributeType(ScalarAttributeType.S);

    final KeySchema keySchema = new KeySchema()
      .withHashKeyElement(hashKey);
    
    
    assertEquals(Long.valueOf(0L), tableDescription.getItemCount());
    assertEquals(keySchema, tableDescription.getKeySchema());
    assertEquals(tableName, tableDescription.getTableName());
  }
  
  @Test
  public void testDeleteTable() {
    final DeleteTableRequest request = new DeleteTableRequest();
    request.setTableName(hashKeyOnlyTableName);
    
    final DeleteTableResult result = db.deleteTable(request);
    assertNotNull(result);
    
    final String tableStatus = result.getTableDescription().getTableStatus();
    assertEquals("DELETING", tableStatus.toString());
    
    final ListTablesResult listTablesResult = db.listTables();
    
    assertFalse(listTablesResult.getTableNames().contains(hashKeyOnlyTableName));
  }
  
  @Test
  public void testListTables() {
    final ListTablesResult result = db.listTables();
    final List<String> tableNames = result.getTableNames();
    assertTrue("Table result should contain table names", tableNames.contains(hashKeyOnlyTableName));
    
    final ListTablesRequest request = new ListTablesRequest();
    final ListTablesResult resultWithRequest = db.listTables(request);
    final List<String> tableNamesWithRequest = resultWithRequest.getTableNames();
    assertTrue("Table result should contain table names", tableNamesWithRequest.contains(hashKeyOnlyTableName));
  }
  
  @Test
  public void testPutItem() {
    PutItemResult result = putItem(hashKeyOnlyTableName, item);
    assertNotNull(result);
  }
  
  @Test
  public void testUpdateItem() {
    putItem(hashKeyOnlyTableName, item);
    
    UpdateItemRequest request = new UpdateItemRequest();
    final Key key = new Key();
    final AttributeValue hashKey = new AttributeValue().withS(itemHashKeyValue);
    final Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
    final AttributeValue newValue = new AttributeValue().withS("some new value");
    
    attributeUpdates.put(
        itemStringAttributeName, 
        new AttributeValueUpdate()
        .withAction(AttributeAction.PUT)
        .withValue(newValue)
    );
    
    key.setHashKeyElement(hashKey);
    
    request
      .withTableName(hashKeyOnlyTableName)
      .withKey(key)
      .withAttributeUpdates(attributeUpdates);
    
    UpdateItemResult result = db.updateItem(request);
    assertNotNull(result);
    
    final GetItemResult itemResult = getItem(itemHashKeyValue);
    final Map<String, AttributeValue> updatedItem = itemResult.getItem();
    
    assertEquals(newValue, updatedItem.get(itemStringAttributeName));
  }
  
  @Test
  public void testUpdateItemNonExistent() {
    UpdateItemRequest request = new UpdateItemRequest();
    final Key key = new Key();
    final AttributeValue hashKey = new AttributeValue().withS(itemHashKeyValue);
    final Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
    final AttributeValue newValue = new AttributeValue().withS("some new value");
    
    attributeUpdates.put(
        itemStringAttributeName, 
        new AttributeValueUpdate()
        .withAction(AttributeAction.PUT)
        .withValue(newValue)
    );
    
    key.setHashKeyElement(hashKey);
    
    request
      .withTableName(hashKeyOnlyTableName)
      .withKey(key)
      .withAttributeUpdates(attributeUpdates);
    
    UpdateItemResult result = db.updateItem(request);
    assertNotNull(result);
    
    final GetItemResult itemResult = getItem(itemHashKeyValue);
    final Map<String, AttributeValue> updatedItem = itemResult.getItem();
    
    assertEquals(newValue, updatedItem.get(itemStringAttributeName));
  }
  
  @Test
  public void testGetItemHashKeyOnly() {
    putItem(hashKeyOnlyTableName, item);
    
    GetItemResult result = getItem(itemHashKeyValue);
    assertEquals(item, result.getItem());
  }

  @Test
  public void testGetItemHashAndRange() {
    item.put(rangeKeyName, new AttributeValue().withS(itemRangeKeyValue));
    putItem(hashAndRangeTableName, item);
    
    final GetItemRequest request = new GetItemRequest();
    final Key key = new Key();
    final AttributeValue hashKey = new AttributeValue().withS(itemHashKeyValue);
    final AttributeValue rangeKey = new AttributeValue().withS(itemRangeKeyValue);
    
    key
      .withHashKeyElement(hashKey)
      .withRangeKeyElement(rangeKey);
    
    
    request
      .withTableName(hashAndRangeTableName)
      .withKey(key);
    
    GetItemResult result = db.getItem(request);
    assertEquals(item, result.getItem());
  }
  
  public void testBatchGetItem() {
    final List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    final Map<String, AttributeValue> item1 = expectedItems.get(0);
    final Map<String, AttributeValue> item2 = expectedItems.get(1);
    final Map<String, AttributeValue> item3 = expectedItems.get(2);
    
    final BatchGetItemRequest request = new BatchGetItemRequest();
    final Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
    final KeysAndAttributes keysAndAttributes = new KeysAndAttributes();

    final Key item1Key = new Key()
      .withHashKeyElement(item1.get(hashKeyName))
      .withRangeKeyElement(item1.get(rangeKeyName));
    final Key item3Key = new Key()
      .withHashKeyElement(item3.get(hashKeyName))
      .withRangeKeyElement(item3.get(rangeKeyName));
    
    keysAndAttributes.withKeys(item1Key, item3Key);
    requestItems.put(hashAndRangeTableName, keysAndAttributes);
    
    request.setRequestItems(requestItems);
    
    final BatchGetItemResult result = db.batchGetItem(request);
    final List<Map<String, AttributeValue>> items = result.getResponses().get(hashAndRangeTableName).getItems();
    
    assertTrue(items.contains(item1));
    assertFalse(items.contains(item2));
    assertTrue(items.contains(item3));
    
    assertTrue(result.getUnprocessedKeys().isEmpty());
  }
  
  @Test(expected=AmazonServiceException.class)
  public void testBatchGetItemNoKeys() {
    final BatchGetItemRequest request = new BatchGetItemRequest();
    final Map<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
    final KeysAndAttributes keysAndAttributes = new KeysAndAttributes()
      .withKeys(new ArrayList<Key>());
    requestItems.put(hashAndRangeTableName, keysAndAttributes);
    request.setRequestItems(requestItems);
    
    db.batchGetItem(request);
  }
  
  @Test
  public void testBatchWriteItem() {
    final List<Map<String, AttributeValue>> items = setupQueryItems(false);
    final BatchWriteItemRequest request = new BatchWriteItemRequest();
    
    final List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
    for(Map<String, AttributeValue> item : items) {
      final PutRequest putRequest = new PutRequest().withItem(item);
      writeRequests.add(new WriteRequest().withPutRequest(putRequest));
    }
    
    final Map<String, List<WriteRequest>> writeItems = new HashMap<String, List<WriteRequest>>();
    writeItems.put(hashAndRangeTableName, writeRequests);
    
    request.withRequestItems(writeItems);
    
    final BatchWriteItemResult result = db.batchWriteItem(request);
    final BatchWriteResponse response = result.getResponses().get(hashAndRangeTableName);
    assertNotNull(response);

    for(Map<String, AttributeValue> item : items) {
      final GetItemResult itemResult = getItem(item.get(hashKeyName).getS(), item.get(rangeKeyName).getS());
      assertEquals(item, itemResult.getItem());
    }
  }
  
  @Test
  public void testBatchWriteItemDelete() {
    final List<Map<String, AttributeValue>> items = setupQueryItems();
    final BatchWriteItemRequest request = new BatchWriteItemRequest();
    
    final List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
    for(Map<String, AttributeValue> item : items) {
      final Key key = new Key()
        .withHashKeyElement(item.get(hashKeyName))
        .withRangeKeyElement(item.get(rangeKeyName));
      final DeleteRequest deleteRequest = new DeleteRequest().withKey(key);
      writeRequests.add(new WriteRequest().withDeleteRequest(deleteRequest));
    }
    
    final Map<String, List<WriteRequest>> writeItems = new HashMap<String, List<WriteRequest>>();
    writeItems.put(hashAndRangeTableName, writeRequests);
    
    request.withRequestItems(writeItems);
    
    final BatchWriteItemResult result = db.batchWriteItem(request);
    final BatchWriteResponse response = result.getResponses().get(hashAndRangeTableName);
    assertNotNull(response);

    for(Map<String, AttributeValue> item : items) {
      final GetItemResult itemResult = getItem(item.get(hashKeyName).getS(), item.get(rangeKeyName).getS());
      assertNull(itemResult.getItem());
    }
  }
  
  @Test(expected=AmazonServiceException.class)
  public void testBatchWriteItemLengthConstraint() {
    final BatchWriteItemRequest request = new BatchWriteItemRequest();
    
    final List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
    for(int i = 0; i<30; i++) {
      final PutRequest putRequest = new PutRequest().withItem(new HashMap<String, AttributeValue>());
      writeRequests.add(new WriteRequest().withPutRequest(putRequest));
    }
    
    final Map<String, List<WriteRequest>> writeItems = new HashMap<String, List<WriteRequest>>();
    writeItems.put(hashAndRangeTableName, writeRequests);
    request.withRequestItems(writeItems);
    
    db.batchWriteItem(request);
  }
  
  @Test
  public void testQueryNoRangeCondition() {
    List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    Map<String, AttributeValue> item = expectedItems.get(0);
    Map<String, AttributeValue> item2 = expectedItems.get(1);
    Map<String, AttributeValue> notFoundItem = expectedItems.get(2);
    
    final QueryRequest request = new QueryRequest();
    request
      .withHashKeyValue(item.get(hashKeyName))
      .withTableName(hashAndRangeTableName);
    
    final QueryResult result = db.query(request);
    
    final List<Map<String, AttributeValue>> items = result.getItems();
    assertTrue(items.contains(item));
    assertTrue(items.contains(item2));
    assertFalse(items.contains(notFoundItem));
    assertEquals(Integer.valueOf(2), result.getCount());
  }
  
  @Test
  public void testQueryEQRangeCondition() {
    List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    Map<String, AttributeValue> notFoundItem = expectedItems.get(0);
    Map<String, AttributeValue> foundItem = expectedItems.get(1);
    Map<String, AttributeValue> notFoundItem2 = expectedItems.get(2);
    
    final QueryRequest request = new QueryRequest();
    final Condition condition = new Condition();
    condition
      .withAttributeValueList(foundItem.get(rangeKeyName))
      .withComparisonOperator(ComparisonOperator.EQ);
    
    request
      .withTableName(hashAndRangeTableName)
      .withHashKeyValue(item.get(hashKeyName))
      .withRangeKeyCondition(condition);
    
    final QueryResult result = db.query(request);
    
    final List<Map<String, AttributeValue>> items = result.getItems();
    assertTrue(items.contains(foundItem));
    assertFalse(items.contains(notFoundItem));
    assertFalse(items.contains(notFoundItem2));
    assertEquals(Integer.valueOf(1), result.getCount());
  }
  
  @Test
  public void testScanNoCondition() {
    List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    Map<String, AttributeValue> item1 = expectedItems.get(0);
    Map<String, AttributeValue> item2 = expectedItems.get(1);
    Map<String, AttributeValue> item3 = expectedItems.get(2);
    
    ScanRequest request = new ScanRequest();
    request
      .withTableName(hashAndRangeTableName)
      .withScanFilter(new HashMap<String, Condition>());
    
    ScanResult result = db.scan(request);
    
    List<Map<String, AttributeValue>> items = result.getItems();
    assertTrue(items.contains(item1));
    assertTrue(items.contains(item2));
    assertTrue(items.contains(item3));
    
    assertEquals(Integer.valueOf(3), result.getCount());
    assertEquals(Integer.valueOf(3), result.getScannedCount());
  }
  
  @Test
  public void testScanContains() {
    final List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    final Map<String, AttributeValue> item1 = expectedItems.get(0);
    final Map<String, AttributeValue> item2 = expectedItems.get(1);
    final Map<String, AttributeValue> item3 = expectedItems.get(2);

    final Map<String, Condition> scanFilter = new HashMap<String, Condition>();
    scanFilter.put(
        itemStringAttributeName, 
        new Condition()
          .withAttributeValueList(new AttributeValue().withS("findthisstring"))
          .withComparisonOperator(ComparisonOperator.CONTAINS)
    );
    
    final ScanRequest request = new ScanRequest();
    request
      .withTableName(hashAndRangeTableName)
      .withScanFilter(scanFilter);
    
    ScanResult result = db.scan(request);
    final List<Map<String, AttributeValue>> items = result.getItems();
    assertFalse(items.contains(item1));
    assertTrue(items.contains(item2));
    assertFalse(items.contains(item3));
    
    assertEquals(Integer.valueOf(1), result.getCount());
    assertEquals(Integer.valueOf(3), result.getScannedCount());
  }
  
  @Test
  public void testScanNotContains() {
    final List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    final Map<String, AttributeValue> item1 = expectedItems.get(0); 
    final Map<String, AttributeValue> item2 = expectedItems.get(1); //contains value findthisstring
    final Map<String, AttributeValue> item3 = expectedItems.get(2); // does not have the scanned attribute value

    final Map<String, Condition> scanFilter = new HashMap<String, Condition>();
    scanFilter.put(
        itemStringAttributeName, 
        new Condition()
          .withAttributeValueList(new AttributeValue().withS("findthisstring"))
          .withComparisonOperator(ComparisonOperator.NOT_CONTAINS)
    );
    
    final ScanRequest request = new ScanRequest();
    request
      .withTableName(hashAndRangeTableName)
      .withScanFilter(scanFilter);
    
    ScanResult result = db.scan(request);
    final List<Map<String, AttributeValue>> items = result.getItems();
    assertTrue(items.contains(item1));
    assertFalse(items.contains(item2));
    assertFalse(items.contains(item3));
    
    assertEquals(Integer.valueOf(1), result.getCount());
    assertEquals(Integer.valueOf(3), result.getScannedCount());
  }
  
  @Test
  public void testScanNull() {
    final List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    final Map<String, AttributeValue> item1 = expectedItems.get(0); 
    final Map<String, AttributeValue> item2 = expectedItems.get(1); //contains value findthisstring
    final Map<String, AttributeValue> item3 = expectedItems.get(2); // does not have the scanned attribute value

    final Map<String, Condition> scanFilter = new HashMap<String, Condition>();
    scanFilter.put(
        itemStringAttributeName, 
        new Condition()
          .withAttributeValueList(new AttributeValue().withS("findthisstring"))
          .withComparisonOperator(ComparisonOperator.NULL)
    );
    
    final ScanRequest request = new ScanRequest();
    request
      .withTableName(hashAndRangeTableName)
      .withScanFilter(scanFilter);
    
    ScanResult result = db.scan(request);
    final List<Map<String, AttributeValue>> items = result.getItems();
    assertFalse(items.contains(item1));
    assertFalse(items.contains(item2));
    assertTrue(items.contains(item3));
    
    assertEquals(Integer.valueOf(1), result.getCount());
    assertEquals(Integer.valueOf(3), result.getScannedCount());
  }
  
  
  @Test
  public void testScanNotNull() {
    final List<Map<String, AttributeValue>> expectedItems = setupQueryItems();
    final Map<String, AttributeValue> item1 = expectedItems.get(0); 
    final Map<String, AttributeValue> item2 = expectedItems.get(1); //contains value findthisstring
    final Map<String, AttributeValue> item3 = expectedItems.get(2); // does not have the scanned attribute value

    final Map<String, Condition> scanFilter = new HashMap<String, Condition>();
    scanFilter.put(
        itemStringAttributeName, 
        new Condition()
          .withAttributeValueList(new AttributeValue().withS("findthisstring"))
          .withComparisonOperator(ComparisonOperator.NOT_NULL)
    );
    
    final ScanRequest request = new ScanRequest();
    request
      .withTableName(hashAndRangeTableName)
      .withScanFilter(scanFilter);
    
    final ScanResult result = db.scan(request);
    final List<Map<String, AttributeValue>> items = result.getItems();
    assertTrue(items.contains(item1));
    assertTrue(items.contains(item2));
    assertFalse(items.contains(item3));
    
    assertEquals(Integer.valueOf(2), result.getCount());
    assertEquals(Integer.valueOf(3), result.getScannedCount());
  }
  
  @Test
  public void testDeleteItem() {
    putItem(hashKeyOnlyTableName, item);
    
    final DeleteItemRequest request = new DeleteItemRequest();
    final Key key = new Key();
    final AttributeValue hashKey = new AttributeValue().withS(itemHashKeyValue);
    
    key.setHashKeyElement(hashKey);
    
    request
      .withTableName(hashKeyOnlyTableName)
      .withKey(key);
    
    DeleteItemResult result = db.deleteItem(request);
    assertNotNull(result);
    
    GetItemResult getItemResult = getItem(itemHashKeyValue);
    assertNull(getItemResult.getItem());
  }
  
  private CreateTableResult createTable(final String tableName, final String hashKeyName) {
    return createTable(tableName, hashKeyName, null);
  }
  /**
   * Helper method to create tables
   * @param tableName
   * @param hashKeyName
   * @return
   */
  private CreateTableResult createTable(final String tableName, final String hashKeyName, final String rangeKeyName) {
    final CreateTableRequest request = new CreateTableRequest();
    final KeySchemaElement hashKey = new KeySchemaElement()
      .withAttributeName(hashKeyName)
      .withAttributeType(ScalarAttributeType.S);
    
    final KeySchema keySchema = new KeySchema()
      .withHashKeyElement(hashKey);

    if (rangeKeyName != null) {
      final KeySchemaElement rangeKey = new KeySchemaElement()
        .withAttributeName(rangeKeyName)
        .withAttributeType(ScalarAttributeType.S);
      
      keySchema.setRangeKeyElement(rangeKey);
    }
  
  
    request
      .withTableName(tableName)
      .withKeySchema(keySchema);
  
    return db.createTable(request);
  }
  
  private PutItemResult putItem(final String tableName, Map<String, AttributeValue> item) {
    final PutItemRequest request = new PutItemRequest();
    request
      .withItem(item)
      .withTableName(tableName);
    
    return db.putItem(request);
  }
  
  private List<Map<String, AttributeValue>> setupQueryItems() {
    return setupQueryItems(true);
  }
  
  private List<Map<String, AttributeValue>> setupQueryItems(final boolean store) {
    final Map<String, AttributeValue> item2 = new HashMap<String, AttributeValue>();
    for(Entry<String, AttributeValue> entry : item.entrySet()) {
      item2.put(entry.getKey(), entry.getValue());
    }
    
    item.put(rangeKeyName, new AttributeValue().withS(itemRangeKeyValue));
    
    final AttributeValue item2RangeKey = new AttributeValue().withS("item 2 range key");
    item2.put(rangeKeyName, item2RangeKey);
    item2.put(itemStringAttributeName, new AttributeValue().withS("new item findthisstring string attribute value"));
    
    
    final Map<String, AttributeValue> notFoundItem = new HashMap<String, AttributeValue>();
    notFoundItem.put(hashKeyName, new AttributeValue().withS("something else that shouldn't be found"));
    notFoundItem.put(rangeKeyName, new AttributeValue().withS("not found range key name"));
    
    
    if (store) {
      putItem(hashAndRangeTableName, item);
      putItem(hashAndRangeTableName, item2);
      putItem(hashAndRangeTableName, notFoundItem);  
    }
    
    final List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
    items.add(item);
    items.add(item2);
    items.add(notFoundItem);
    
    return items;
  }
  
  private GetItemResult getItem(final String hashKeyValue) {
    return getItem(hashKeyValue, null);
  }
  
  private GetItemResult getItem(final String hashKeyValue, final String rangeKeyValue) {
    final GetItemRequest request = new GetItemRequest();
    final Key key = new Key();
    final AttributeValue hashKey = new AttributeValue().withS(hashKeyValue);
    key.setHashKeyElement(hashKey);
    
    if (rangeKeyValue != null) {
      final AttributeValue rangeKey = new AttributeValue().withS(rangeKeyValue);
      key.setRangeKeyElement(rangeKey);
    }
    
    String tableName;
    if (rangeKeyValue != null) {
      tableName = hashAndRangeTableName;
    } else {
      tableName = hashKeyOnlyTableName;
    }
    
    request
      .withTableName(tableName)
      .withKey(key);
    
    GetItemResult result = db.getItem(request);
    return result;
  }

  
}
