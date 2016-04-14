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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;

/**
 * (graphId, sourceIdId, targetId, integerLabel) |><| (integerLabel,stringLabel)
 * => (graphId, sourceIdId, targetId, stringLabel)
 */
public class EdgeLabelDecoder
  implements JoinFunction<Tuple4<GradoopId, GradoopId, GradoopId, Integer>,
  Tuple2<Integer, String>, Tuple4<GradoopId, GradoopId, GradoopId, String>> {
  @Override
  public Tuple4<GradoopId, GradoopId, GradoopId, String> join(
    Tuple4<GradoopId, GradoopId, GradoopId, Integer> gidSidTidLabel,
    Tuple2<Integer, String> dictionaryEntry
  ) throws Exception {

    return new Tuple4<>(gidSidTidLabel.f0,
      gidSidTidLabel.f1, gidSidTidLabel.f2, dictionaryEntry.f1);
  }
}