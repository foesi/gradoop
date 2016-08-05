/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.projection.common;

import com.google.common.collect.Maps;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.Serializable;
import java.util.Map;

/**
 * The Bindingextractor class is for extracting the mapping of a GraphElement
 * which variable is bound by which Id.
 */
public class BindingExtractor implements Serializable {

  /**
   * Name of Property for bindings
   */
  private final String bindingsString;

  /**
   * @param bindingsString name of the Property where the bindings can be found.
   */
  public BindingExtractor(final String bindingsString) {
    this.bindingsString = bindingsString;
  }

  /**
   * @param epgmElement GraphElement to get The bindings map for.
   * @return Map of Ids to bound variables
   */
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
