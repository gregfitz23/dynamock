package com.bizo.aws.dynamock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.model.AttributeAction;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchResponse;
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
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeysAndAttributes;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.PutRequest;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.amazonaws.services.dynamodb.model.UpdateTableResult;
import com.amazonaws.services.dynamodb.model.WriteRequest;
import com.bizo.aws.dynamock.hashmap.DynamockDBTableManagerHashMapImpl;

/**
 * A client for mocking AmazonDynamoDBClient locally, removing the need for a network connection during testing.
 * Current underlying table implementations include:
 *  DynamockDBTableHashMapImpl - an implementation using in-memory HashMaps to maintain data state.
 *   
 * @author gregfitzgerald
 *
 */
public class DynamockDBClient implements AmazonDynamoDB {

  private DynamockDBTableManager tableManager;
  
  public DynamockDBClient() {
    this(new DynamockDBTableManagerHashMapImpl());
  }
  
  public DynamockDBClient(final DynamockDBTableManager tableManager) {
    super();
    this.tableManager = tableManager;
  }
  
  @Override
  public void setEndpoint(String endpoint) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Perform the scan request on the given table
   */
  @Override
  public ScanResult scan(ScanRequest scanRequest)
      throws AmazonServiceException, AmazonClientException {

    final DynamockDBTable table = getTable(scanRequest.getTableName());
    final Map<String, Condition> scanFilter = scanRequest.getScanFilter();
    
    Collection<DynamockDBItem> itemObjs = table.getAllItems();
    final Integer originalCount = itemObjs.size();
    
    // for each entry in the scan filter create a map 
    // of AttributeValue (as identified by the key of the scanfilter) -> Item
    // and evaluate that map against the condition
    for (Entry<String, Condition> filter : scanFilter.entrySet()) {
      final String attributeName = filter.getKey();
      final Condition condition = filter.getValue();

      // valueItemMap - a list of items to be scanned, keyed by the AttributeValue to be scanned
      final Map<AttributeValue, DynamockDBItem> valueItemMap = new HashMap<AttributeValue, DynamockDBItem>();
      for (DynamockDBItem item: itemObjs) {
        final AttributeValue value = item.getAttributeValue(attributeName);
        valueItemMap.put(value, item);
      }
      itemObjs = filterByCondition(valueItemMap, condition);
    }

    // convert the matching DynamockDBItems into Map<String, AttributeValue>
    final List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
    for(DynamockDBItem itemObj : itemObjs) {
      items.add(itemObj.toMap());
    }

    // build and return the ScanResult
    final ScanResult result = new ScanResult()
      .withItems(items)
      .withCount(items.size())
      .withScannedCount(originalCount);
    
    return result;
    
  }

  @Override
  public CreateTableResult createTable(CreateTableRequest createTableRequest)
      throws AmazonServiceException, AmazonClientException {
    
    final String tableName = createTableRequest.getTableName();
    final KeySchema keySchema = createTableRequest.getKeySchema();
    
//    try {
      // setup table and keys
      tableManager.createTable(tableName, keySchema);
//    } catch (Exception e) {
//      throw new AmazonClientException(e.getMessage());
//    }

    final CreateTableResult result = new CreateTableResult();
    final TableDescription tableDescription = new TableDescription()
      .withItemCount(Long.valueOf(0L))
      .withTableName(tableName)
      .withKeySchema(keySchema);
    
    return result.withTableDescription(tableDescription);
  }

  @Override
  public ListTablesResult listTables(ListTablesRequest listTablesRequest)
      throws AmazonServiceException, AmazonClientException {

    return listTables();
  }

  @Override
  public QueryResult query(final QueryRequest queryRequest)
      throws AmazonServiceException, AmazonClientException {
    
    final DynamockDBTable table = getTable(queryRequest.getTableName());
    final AttributeValue hashKey = queryRequest.getHashKeyValue();
    
    // must have a hash key to perform query
    if (hashKey == null) {
      throw new AmazonClientException("Hash key was null");
    }

    // find all the items by range key for this query (return an empty list if none exists)
    Map<AttributeValue, DynamockDBItem> itemsByRange = table.getItemsForHashKey(hashKey);
    if (itemsByRange == null) {
      itemsByRange = new HashMap<AttributeValue, DynamockDBItem>();
    }
    
    // filter the list on range key by the given condition and massage it back into Map form
    final Collection<DynamockDBItem> items = filterByCondition(itemsByRange, queryRequest.getRangeKeyCondition());
    final List<Map<String, AttributeValue>> retItems = new ArrayList<Map<String, AttributeValue>>();
    for(DynamockDBItem item : items) {
      retItems.add(item.toMap());
    }
    
    // sort by range key
    Boolean scanIndexForward = queryRequest.getScanIndexForward();
    if (scanIndexForward == null) {
      scanIndexForward = Boolean.TRUE;
    }
    
    sortResultList(retItems, 
        table.getRangeKeyName(), 
        scanIndexForward);
    
    // build the result
    final QueryResult result = new QueryResult()
      .withItems(retItems)
      .withCount(retItems.size());
    
    return result;
  }

  @Override
  public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest)
      throws AmazonServiceException, AmazonClientException {
    
    final Map<String, AttributeValueUpdate> updates = updateItemRequest.getAttributeUpdates();
    final DynamockDBTable table = getTable(updateItemRequest.getTableName());
    final Key key = updateItemRequest.getKey();
    DynamockDBItem item = table.getItem(key);
    
    // if item is not found, create it
    if (item == null) {
      final Map<String, AttributeValue> keyMap = new HashMap<String, AttributeValue>();
      
      // hashkey
      keyMap.put(
          table.getHashKeyName(), 
          updateItemRequest.getKey().getHashKeyElement()
      );
      //rangekey
      if (table.getRangeKeyName() != null) {
        keyMap.put(
            table.getRangeKeyName(), 
            updateItemRequest.getKey().getRangeKeyElement()
        );
      }
      
      item = table.createItem(keyMap);
    }
    
    // set the updates here
    for (Entry<String, AttributeValueUpdate> entry : updates.entrySet()) {
      final String attributeName = entry.getKey();
      final AttributeValueUpdate update = entry.getValue();
      
      if (update.getAction().equals(AttributeAction.PUT.toString())) {
        item.setAttributeValue(attributeName, update.getValue());
      }
    }
    
    table.putItem(key, item);
    
    return new UpdateItemResult();
  }

  @Override
  public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
      throws AmazonServiceException, AmazonClientException {
    
    throw new UnsupportedOperationException();
  }

  /**
   * Add an item to the table given by putItemRequest.  Items are keyed by the hash and (optional) range in the putItemRequest.
   */
  @Override
  public PutItemResult putItem(PutItemRequest putItemRequest)
      throws AmazonServiceException, AmazonClientException {
    
    final String tableName = putItemRequest.getTableName();
    final DynamockDBTable table = getTable(tableName);
    
    table.createItem(putItemRequest.getItem());
    
    return new PutItemResult();
  }

  @Override
  public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
      throws AmazonServiceException, AmazonClientException {
    
    final String tableName = deleteTableRequest.getTableName();
    // trigger an exception if table doesn't exist
    getTable(tableName);
    tableManager.deleteTable(tableName);
    
    final TableDescription tableDescription = new TableDescription()
      .withTableName(tableName)
      .withTableStatus(TableStatus.DELETING);
    
    final DeleteTableResult result = new DeleteTableResult()
      .withTableDescription(tableDescription);
    
    return result;
  }

  @Override
  public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest)
      throws AmazonServiceException, AmazonClientException {

    final Key key = deleteItemRequest.getKey();
    final DynamockDBTable table = getTable(deleteItemRequest.getTableName());
    
    table.deleteItem(key);
    
    return new DeleteItemResult();
  }

  @Override
  public DescribeTableResult describeTable(
      DescribeTableRequest describeTableRequest) throws AmazonServiceException,
      AmazonClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetItemResult getItem(GetItemRequest getItemRequest)
      throws AmazonServiceException, AmazonClientException {
    final GetItemResult result = new GetItemResult();
    
    final String tableName = getItemRequest.getTableName();
    final DynamockDBTable table = getTable(tableName);
    
    final DynamockDBItem item = table.getItem(getItemRequest.getKey());

    Map<String, AttributeValue> retItem = null;
    if (item != null) {
      retItem = item.toMap();
    }
    
    result.setItem(retItem);
    
    return result;
  }

  /**
   * Get multiple tables and items by keys.
   */
  @Override
  public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest)
      throws AmazonServiceException, AmazonClientException {
    
    final Map<String, KeysAndAttributes> requestItems = batchGetItemRequest.getRequestItems();
    final Map<String, BatchResponse> retMap = new HashMap<String, BatchResponse>();
    
    for (Entry<String, KeysAndAttributes> entry : requestItems.entrySet()) {
      final String tableName = entry.getKey();
      final KeysAndAttributes keysAndAttributes = entry.getValue();
      final Collection<Key> keys = keysAndAttributes.getKeys();
      
      if (keys.isEmpty()) {
        throw new AmazonServiceException("1 validation error detected: Value null at 'requestItems." + tableName + "' failed to satisfy constraint: Member must not be null");
      }
      
      final DynamockDBTable table = getTable(tableName);
      final BatchResponse batchResponse = new BatchResponse();
      final List<Map<String, AttributeValue>> items = new ArrayList<Map<String, AttributeValue>>();
      
      for (Key key : keys) {
        final DynamockDBItem item = table.getItem(key);
        if (item != null) {
          items.add(item.toMap());
        }
      }
      
      batchResponse.setItems(items);
      retMap.put(tableName, batchResponse);
    }
    
    final BatchGetItemResult result = new BatchGetItemResult()
      .withResponses(retMap)
      .withUnprocessedKeys(new HashMap<String, KeysAndAttributes>());
    
    return result;
    
  }
  
  /**
   * Supports batch deletes and puts 
   */
  @Override
  public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) 
      throws AmazonServiceException, AmazonClientException {
    
    final Map<String, List<WriteRequest>> tableRequestItemMap = batchWriteItemRequest.getRequestItems();
    final BatchWriteItemResult result = new BatchWriteItemResult();
    final Map<String, BatchWriteResponse> responses = new HashMap<String, BatchWriteResponse>();
    double totalCount = 0;
    
    for (Entry<String, List<WriteRequest>> entry : tableRequestItemMap.entrySet()) {
      final String tableName = entry.getKey();
      final List<WriteRequest> writeRequests = entry.getValue();
      final DynamockDBTable table = getTable(tableName);
      final BatchWriteResponse response = new BatchWriteResponse();
      double count = 0;
      
      
      for (WriteRequest writeRequest : writeRequests) {
        final PutRequest putRequest = writeRequest.getPutRequest();
        final DeleteRequest deleteRequest = writeRequest.getDeleteRequest();
        
        //puts
        if (putRequest != null) {
          Map<String, AttributeValue> item = putRequest.getItem();
          table.createItem(item);
          count++;
          totalCount++;
        }
        
        //deletes
        if (deleteRequest != null) {
          final Key key = deleteRequest.getKey();
          table.deleteItem(key);
          count++;
          totalCount++;
        }
      }
      
      response.setConsumedCapacityUnits(count);
      responses.put(tableName, response);
    }
    
    if (totalCount > 25) {
      throw new AmazonServiceException("1 validation error detected: failed to satisfy constraint: Length must be between 1-25");      
    }

    return result.withResponses(responses);
  }


  /**
   * List all tables
   */
  @Override
  public ListTablesResult listTables() throws AmazonServiceException,
      AmazonClientException {
    
    final ListTablesResult result = new ListTablesResult();
    final Collection<DynamockDBTable> tables = tableManager.getTables();

    final List<String> tableNames = new ArrayList<String>();
    for(DynamockDBTable table : tables) {
      tableNames.add(table.getTableName());
    }
    result.setTableNames(tableNames);
    
    return result;
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(
      AmazonWebServiceRequest request) {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Lookup the table by name, throwing an AmazonServiceException if null
   * @param tableName
   * @return
   */
  private DynamockDBTable getTable(final String tableName)
    throws AmazonServiceException {
    
    final DynamockDBTable table = tableManager.getTable(tableName);
    if (table == null) {
      throw new AmazonServiceException(tableName + " does not exist");
    }
    
    return table;
  }
  
  /**
   * Handle the given range condition by applying it to the given RangeKey->DynomockDBItem map.
   * @param items
   * @param condition
   * @return
   */
  private Collection<DynamockDBItem> filterByCondition(Map<AttributeValue, DynamockDBItem> items, Condition condition) {
    if (condition == null || items.isEmpty()) {
      return items.values();
    }
    
    final Collection<DynamockDBItem> retItems = new ArrayList<DynamockDBItem>();
    
    for (Entry<AttributeValue, DynamockDBItem> entry: items.entrySet()) {
      if (matchesCondition(entry.getKey(), condition)) {
        retItems.add(entry.getValue());
      }
    }
    
    return retItems;
  }
  
  /**
   * Return true if the given attribute value is applicable for the given condition.
   * @param attributeValue
   * @param condition
   * @return
   */
  private boolean matchesCondition(final AttributeValue attributeValue, final Condition condition) {
    final List<AttributeValue> targets = condition.getAttributeValueList();
    final ComparisonOperator comparisonOperator = ComparisonOperator.valueOf(condition.getComparisonOperator());
    
    if (attributeValue == null) { // if attribute value is null, the only matching ComparisonOperator is NULL
      return comparisonOperator == ComparisonOperator.NULL;
    }
    
    final String attributeValueStr = getStringForAttributeValue(attributeValue); 
    
    switch(comparisonOperator) {
      case EQ:
        return targets.contains(attributeValue);
      case CONTAINS:
        for (AttributeValue target : targets) {
          final String targetString = target.getS();
          if (attributeValueStr.contains(targetString)) {
            return true;
          }
        }
        return false;
      case NOT_CONTAINS:
        for (AttributeValue target : targets) {
          final String targetString = target.getS();
          if (attributeValueStr.contains(targetString)) {
            return false;
          }
        }
        return true;
      case NOT_NULL:
        return true; //null values are weeded out above.
      case NULL:
        return false; //null values are weeded out above.
      default: 
        throw new java.lang.UnsupportedOperationException("Query or scan with " + comparisonOperator);  
    }
  }

  /**
   * Convert an AttributeValue into it's string or number value.
   * @param attributeValue
   * @return
   */
  private String getStringForAttributeValue(AttributeValue attributeValue) {
    if (attributeValue.getS() != null) {
      return attributeValue.getS();
    }
    
    return attributeValue.getN();
  }
  
  /**
   * Sort the given list by the given field.
   * @param retItems
   * @param sortFieldName
   * @param scanIndexForward
   */
  private void sortResultList(final List<Map<String, AttributeValue>> retItems,
      final String sortFieldName, final Boolean scanIndexForward) {
    
    Collections.sort(retItems, new Comparator<Map<String, AttributeValue>>() {
      @Override
      public int compare(Map<String, AttributeValue> a, Map<String, AttributeValue> b) {
        final String aStr = getStringForAttributeValue(a.get(sortFieldName));
        final String bStr = getStringForAttributeValue(b.get(sortFieldName));
        
        if (scanIndexForward) {
          return aStr.compareTo(bStr);
        } else {
          return bStr.compareTo(aStr);
        }
      }
    });
  }
}
