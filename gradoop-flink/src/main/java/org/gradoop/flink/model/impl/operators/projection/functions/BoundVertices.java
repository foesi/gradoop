package org.gradoop.flink.model.impl.operators.projection.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.projection.common
  .BindingExtractor;

public class BoundVertices implements FilterFunction<Vertex> {

  private final String productionPattern;

  public BoundVertices(final String production) {
    this.productionPattern = production;
  }

  @Override
  public boolean filter(Vertex vertex) throws Exception {
    return new BindingExtractor(productionPattern)
      .getBindings(vertex).isEmpty();
  }
}
