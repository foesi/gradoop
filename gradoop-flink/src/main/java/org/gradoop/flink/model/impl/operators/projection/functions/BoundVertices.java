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

package org.gradoop.flink.model.impl.operators.projection.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.projection.common
  .BindingExtractor;

/**
 * Filter Function for bound Vertices
 */
public class BoundVertices implements FilterFunction<Vertex> {

  /**
   * Production Pattern to get bound variables
   */
  private final String productionPattern;

  /**
   * @param production Production Pattern
   */
  public BoundVertices(final String production) {
    this.productionPattern = production;
  }

  @Override
  public boolean filter(Vertex vertex) throws Exception {
    return new BindingExtractor(productionPattern)
      .getBindings(vertex).isEmpty();
  }
}
