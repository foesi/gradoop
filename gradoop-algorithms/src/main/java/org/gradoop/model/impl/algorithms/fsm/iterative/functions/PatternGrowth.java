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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.PatternGrower;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.Transaction;


import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Core of gSpan implementation. Grows embeddings of KnownToBeGloballyFrequent DFS codes.
 */
public class PatternGrowth
  implements MapFunction<Transaction, Transaction> {

  private final PatternGrower grower;

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public PatternGrowth(FSMConfig fsmConfig) {
    this.grower = new PatternGrower(fsmConfig);
  }

  @Override
  public Transaction map(Transaction transaction)
    throws Exception {

    if (! transaction.isCollector()) {
      growFrequentDfsCodeEmbeddings(transaction);
    }

    return transaction;
  }

  /**
   * grows all embeddings of frequent DFS codes in a graph
   * @param graph graph search space item
   * @return graph with grown embeddings
   */
  private Transaction growFrequentDfsCodeEmbeddings(
    Transaction graph) {

    Map<Integer, AdjacencyList> adjacencyLists = graph.getAdjacencyLists();

    Map<CompressedDFSCode, Collection<DFSEmbedding>> parentEmbeddings =
      graph.getCodeEmbeddings();

    HashMap<CompressedDFSCode, Collection<DFSEmbedding>> childEmbeddings =
      grower.growEmbeddings(adjacencyLists, parentEmbeddings);

    graph.setCodeEmbeddings(childEmbeddings);
    graph.setActive(! childEmbeddings.isEmpty());

    return graph;
  }
}
