package org.gradoop.flink.model.impl.operators.projection.common;

import com.google.common.collect.Maps;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.Serializable;
import java.util.Map;

public class BindingExtractor implements Serializable {

  private final String bindingsString;

  public BindingExtractor(final String bindingsString) {
    this.bindingsString = bindingsString;
  }

  public Map<String, String> getBindings(Element epgmElement) {
    PropertyValue bindingProp = epgmElement.getProperties().get(bindingsString);

    Map<String, String> result = Maps.newHashMap();

    if (bindingProp == null) {
      return result;
    }

    String propertyString = bindingProp.getString();

    String[] bindings = propertyString.split(",");

    for (String binding : bindings) {
      String[] splittedBinding = binding.split(":");
      result.put(splittedBinding[0], splittedBinding[1]);
    }

    return result;
  }
}
