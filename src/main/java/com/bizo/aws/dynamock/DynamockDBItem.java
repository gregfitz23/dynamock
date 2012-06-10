package com.bizo.aws.dynamock;

import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;

/**
 * An interface representing DynamockDB items.  Provides accessors for attribute value and toMap implementation.
 * @author gregfitzgerald
 *
 */
public interface DynamockDBItem {

  public abstract Map<String, AttributeValue> toMap();

  public abstract AttributeValue getAttributeValue(final String attribute);

  public abstract void setAttributeValue(final String attribute,
      final AttributeValue value);

}