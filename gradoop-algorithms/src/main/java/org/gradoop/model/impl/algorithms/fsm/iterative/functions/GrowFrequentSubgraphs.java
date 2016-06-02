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

package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

/**
 * Core of gSpan implementation. Grows embeddings of KnownToBeGloballyFrequent DFS codes.
 */
public class GrowFrequentSubgraphs
  extends RichMapFunction<IterationItem, IterationItem> {

  private final FSMConfig fsmConfig;
  /**
   * frequent DFS codes
   */
  private Collection<DfsCode> frequentSubgraphs;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Collection<Supportable<CompressedDfsCode>> compressedSubgraphs =
      getRuntimeContext().getBroadcastVariable(BroadcastNames.FREQUENT_SUBGRAPHS);

    frequentSubgraphs = Lists.newArrayList();

    for (Supportable<CompressedDfsCode> compressedSubgraph : compressedSubgraphs) {
      frequentSubgraphs.add(compressedSubgraph.getObject().getDfsCode());
    }

  }
  /**
   * constructor
   * @param fsmConfig configuration
   */
  public GrowFrequentSubgraphs(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public IterationItem map(IterationItem wrapper) throws Exception {

    if (! wrapper.isCollector()) {
      GSpan.growFrequentSubgraphs(
        wrapper.getTransaction(), frequentSubgraphs, fsmConfig);
    }

    return wrapper;
  }
}
