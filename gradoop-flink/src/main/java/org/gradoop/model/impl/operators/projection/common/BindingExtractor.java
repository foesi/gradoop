package org.gradoop.model.impl.operators.projection.common;

import com.google.common.collect.Maps;
import org.gradoop.model.api.EPGMElement;

import java.io.Serializable;
import java.util.Map;

public class BindingExtractor implements Serializable {

  private final String bindingsString;

  public BindingExtractor(final String bindingsString) {
    this.bindingsString = bindingsString;
  }

  public Map<String, String> getBindings(EPGMElement epgmElement) {
    String propertyString = epgmElement.getProperties().get(bindingsString)
      .getString();

    String[] bindings = propertyString.split(",");

    Map<String, String> result = Maps.newHashMap();

    for (String binding : bindings) {
      String[] splittedBinding = binding.split(":");
      result.put(splittedBinding[0], splittedBinding[1]);
    }

    return result;
  }
}
