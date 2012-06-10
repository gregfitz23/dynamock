package com.bizo.aws.dynamock;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;

/**
 * An implementation of DynamockDBItem utilizing in-memory HashMaps as item stores.
 * @author gregfitzgerald
 *
 */
public class DynamockDBItemHashMapImpl implements DynamockDBItem {
  private Map<String, AttributeValue> attributes = new HashMap<String, AttributeValue>();

  public DynamockDBItemHashMapImpl(Map<String, AttributeValue> attributes) {
    this.attributes = attributes;
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBItem#toMap()
   */
  @Override
  public Map<String, AttributeValue> toMap() {
    return attributes;
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBItem#getAttributeValue(java.lang.String)
   */
  @Override
  public AttributeValue getAttributeValue(final String attribute) {
    return attributes.get(attribute);
  }
  
  /* (non-Javadoc)
   * @see com.bizo.comscore.aws.DynomockDBItem#setAttributeValue(java.lang.String, com.amazonaws.services.dynamodb.model.AttributeValue)
   */
  @Override
  public void setAttributeValue(final String attribute, final AttributeValue value) {
    attributes.put(attribute, value);
  }
}
