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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.Transaction;



import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Core of gSpan implementation. Grows embeddings of KnownToBeGloballyFrequent DFS codes.
 */
public class SupportPruning extends
  RichMapFunction<Transaction, Transaction> {

  /**
   * frequent DFS codes
   */
  private Collection<CompressedDFSCode> frequentDfsCodes;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Collection<CompressedDFSCode>> broadcast = getRuntimeContext()
      .getBroadcastVariable(BroadcastNames.FREQUENT_PATTERNS);

    if (broadcast.isEmpty()) {
      this.frequentDfsCodes = null;
    } else {
      this.frequentDfsCodes = broadcast.get(0);
    }
  }

  @Override
  public Transaction map(Transaction transaction) throws Exception {

    if (frequentDfsCodes != null) {
      if (transaction.isCollector()) {
        transaction = updateCollector(transaction, frequentDfsCodes);
      } else {
        transaction = dropInfrequentEmbeddings(transaction, frequentDfsCodes);
      }
    }
    return transaction;
  }

  /**
   * appends frequent DFS codes collected so far by new ones
   * @param collector collector search space item
   * @param newFrequentDfsCodes new frequent DFS codes
   * @return updated collector
   */
  private Transaction updateCollector(Transaction collector,
    Collection<CompressedDFSCode> newFrequentDfsCodes) {

    collector.getFrequentDfsCodes().addAll(newFrequentDfsCodes);

    return collector;
  }

  /**
   * grows all embeddings of frequent DFS codes in a graph
   * @param graph graph search space item
   * @param frequentDfsCodes frequent DFS codes
   * @return graph with grown embeddings
   */
  private Transaction dropInfrequentEmbeddings(Transaction graph,
    Collection<CompressedDFSCode> frequentDfsCodes) {

    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      graph.getCodeEmbeddings();

    Collection<CompressedDFSCode> supportedCodes = Lists
      .newArrayList(codeEmbeddings.keySet());

    for (CompressedDFSCode supportedCode : supportedCodes) {
      if (! frequentDfsCodes.contains(supportedCode)) {
        codeEmbeddings.remove(supportedCode);
      }
    }

    graph.setActive(! codeEmbeddings.isEmpty());

    return graph;
  }
}
