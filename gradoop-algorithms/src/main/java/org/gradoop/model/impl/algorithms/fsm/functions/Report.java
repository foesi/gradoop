package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpaceItem;

public class Report implements
  FlatMapFunction<SearchSpaceItem, Tuple2<CompressedDfsCode, Integer>> {

  @Override
  public void flatMap(SearchSpaceItem searchSpaceItem,
    Collector<Tuple2<CompressedDfsCode, Integer>> collector) throws Exception {

    for(CompressedDfsCode code : searchSpaceItem.getDfsCodes()) {
      collector.collect(new Tuple2<>(code, 1));
    }
  }
}
