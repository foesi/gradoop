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

package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.functions.tuple.ValueOf1;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.GetPropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.GraphIdsWithPropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Utility method to compute the maximum of a property of elements in a dataset
 * without collecting it.
 */
public class Max {

  /**
   * Computes the maximum of the given property of elements in the given dataset
   * and stores the result in a 1-element dataset.
   *
   * @param dataSet input dataset
   * @param propertyKey key of property
   * @param min minimum of the same type as the property value
   * @param <EL>     element type in input dataset
   * @return 1-element dataset with maximum of input dataset
   */
  public static <EL extends Element> DataSet<PropertyValue> max(
    DataSet<EL> dataSet,
    String propertyKey,
    Number min) {
    return dataSet
      .map(new GetPropertyValue<EL>(propertyKey, min))
      .reduce(new MaxOfPropertyValues(min))
      .map(new ValueOf1<PropertyValue>());
  }

  /**
   * Groups the input dataset by the contained elements and computes the maximum
   * of a property for each group.
   * Returns a {@code Tuple2} containing the group element and the
   * corresponding maximum value.
   *
   * @param dataSet input dataset
   * @param propertyKey key of property
   * @param min minimum, of the same type as the property value
   * @param <EL>     element type in input dataset
   * @return {@code Tuple2} with group value and group maximum
   */
  public static <EL extends GraphElement>
  DataSet<Tuple2<GradoopId, PropertyValue>> groupBy(
    DataSet<EL> dataSet,
    String propertyKey,
    Number min) {
    return dataSet.flatMap(new GraphIdsWithPropertyValue<EL>(propertyKey))
      .groupBy(0)
      .reduceGroup(new MaxOfPropertyValuesGroups(min));
  }
}
