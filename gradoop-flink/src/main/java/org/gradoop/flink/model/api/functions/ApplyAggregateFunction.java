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

package org.gradoop.flink.model.api.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Describes an aggregate function that can be applied on a collection of graphs
 * and computes an aggregate value for each graph contained in the collection.
 *
 * @see ApplyAggregateFunction
 */
public interface ApplyAggregateFunction {

  /**
   * Defines the aggregate function. The input is a graph collection, the output
   * contains a tuple for each graph contained in the collection. The tuple
   * holds the graph identifier and the associated aggregate value (e.g. count).
   *
   * @param collection input graph collection
   * @return data set containing graph id + aggregate tuples
   */
  DataSet<Tuple2<GradoopId, PropertyValue>> execute(GraphCollection collection);

  /**
   * Return the default value that will be used when a graph has no vertices
   * or edges with the specified property.
   * @return default value
   */
  Number getDefaultValue();
}
