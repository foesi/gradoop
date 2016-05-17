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

package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

/**
 * (graphId, vertexId, integerLabel) |><| (integerLabel, stringLabel)
 * => (graphId, vertexId, stringLabel)
 */
public class VertexLabelDecoder
  extends RichMapFunction<Tuple3<GradoopId, GradoopId, Integer>,
  Tuple3<GradoopId, GradoopId, String>> {

  public static final String DICTIONARY = "dictionary";
  private ArrayList<String> dictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.dictionary = getRuntimeContext()
      .<ArrayList<String>>getBroadcastVariable(DICTIONARY)
      .get(0);
  }

  @Override
  public Tuple3<GradoopId, GradoopId, String> map(
    Tuple3<GradoopId, GradoopId, Integer> vertex) throws
    Exception {
    return new Tuple3<>(vertex.f0, vertex.f1, dictionary.get(vertex.f2));
  }

}