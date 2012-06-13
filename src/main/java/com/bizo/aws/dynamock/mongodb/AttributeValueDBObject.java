package com.bizo.aws.dynamock.mongodb;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * An object that serializes AttributeValues as native Strings, Numbers, List<String> and List<Number>
 * @author gregfitzgerald
 *
 */
public class AttributeValueDBObject extends BasicDBObject implements DBObject {

  /**
   * 
   */
  private static final long serialVersionUID = -2066373879400118033L;

  public AttributeValueDBObject() {
    super();
  }
  
  @SuppressWarnings("rawtypes")
  public AttributeValueDBObject(Map attributes) {
    for (Object key : attributes.keySet()) {
      String keyStr = (String)key;
      AttributeValue av = (AttributeValue)attributes.get(keyStr);
      this.put(keyStr, av);
    }
  }
  
  public Object put(String key, AttributeValue val) {
    final NumberFormat format = NumberFormat.getInstance(Locale.US);    
    final String s = val.getS();
    final String n = val.getN();
    final List<String> ss = val.getSS();
    final List<String> ns = val.getNS();
    
    if (s != null) {
      return super.put(key, s);
    } else if (n != null) {
      
      try {
        final Number number = format.parse(n);
        return super.put(key, number);
      } catch (ParseException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (ss != null) {
      return super.put(key, ss);
    } else if (ns != null) {
      final List<Number> numberList = new ArrayList<Number>();
      for (String numberStr : ns) {
        try {
          final Number number = format.parse(numberStr);
          numberList.add(number);
        } catch (ParseException e) {
          throw new IllegalArgumentException(e);
        }
      }
      
      return super.put(key, numberList);
    }
    
    throw new IllegalArgumentException();
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Object get(String key) {
    final Object val = super.get(key);
    final AttributeValue av = new AttributeValue();
    
    if (val instanceof String) {
      return av.withS((String)val);
    } else if (val instanceof Number) {
      return av.withN(val.toString());
    } else if (val instanceof List) { // it's an SS or NS
      Object first = ((List)val).get(0);
      if (first instanceof String) { // it's an SS, add as list
        return av.withSS((List)val);
      } else if (first instanceof Number) { // it's an NS, convert numbers to strings
        final List<String> numberStrings = new ArrayList<String>();
        for (Object number : (List)val) {
          numberStrings.add(number.toString());
        }
        return av.withNS(numberStrings);
      }
    }
    
    return val;
  }
}
