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

package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .GSpanGraph;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;

/**
 * Creates a graph from a set of edge triples.
 */
public class BuildGSpanGraph
  implements GroupReduceFunction<EdgeTriple, GSpanGraph> {

  @Override
  public void reduce(Iterable<EdgeTriple> iterable,
    Collector<GSpanGraph> collector) throws Exception {

    collector.collect(GSpan.createGSpanGraph(iterable));
  }
}
