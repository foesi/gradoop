package org.gradoop.flink.model.impl.operators.projection.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.projection.common
  .BindingExtractor;

public class BoundEdges implements FilterFunction<Edge> {

  private final String productionPattern;

  public BoundEdges(final String production) {
    this.productionPattern = production;
  }

  @Override
  public boolean filter(Edge edge) throws Exception {
    return new BindingExtractor(productionPattern).getBindings(edge).isEmpty();
  }
}
