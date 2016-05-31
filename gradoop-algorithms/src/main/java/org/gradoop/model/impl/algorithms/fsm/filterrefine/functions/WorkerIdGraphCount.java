package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;

import java.util.Collection;


public class WorkerIdGraphCount
  implements MapFunction
  <Tuple2<Integer, Collection<GSpanTransaction>>, Tuple2<Integer, Integer>> {

  @Override
  public Tuple2<Integer, Integer> map(
    Tuple2<Integer, Collection<GSpanTransaction>> pair) throws Exception {
    return new Tuple2<>(pair.f0, pair.f1.size());
  }
}
