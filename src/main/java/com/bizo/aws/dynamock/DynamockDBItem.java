package com.bizo.aws.dynamock;

import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;

/**
 * An interface representing DynamockDB items.  Provides accessors for attribute value and toMap implementation.
 * @author gregfitzgerald
 *
 */
public interface DynamockDBItem {

  public Map<String, AttributeValue> toMap();

  public AttributeValue getAttributeValue(final String attribute);

  public void setAttributeValue(String attribute,
      AttributeValue value);

}