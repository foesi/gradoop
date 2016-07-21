package org.gradoop.model.impl.operators.projection.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.projection.common.BindingExtractor;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Set;

public class ExtendTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends RichMapFunction<GraphTransaction<G, V, E>, LogicalGraph<G, V, E>> {

  private final String productionPattern;
  private QueryHandler productionHandler;
  private final BindingExtractor extractor;

  public ExtendTransaction(String productionPattern, BindingExtractor extractor) {
    this.productionPattern = productionPattern;
    this.extractor = extractor;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.productionHandler = new QueryHandler(productionPattern);
  }

  @Override
  public LogicalGraph<G, V, E> map(
    GraphTransaction<G, V, E> transaction) throws Exception {
    Set<V> vertices = transaction.getVertices();
    Set<E> edges = transaction.getEdges();


    return LogicalGraph.fromCollections();
  }
}
