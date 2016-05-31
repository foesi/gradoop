package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

/**
 * Created by peet on 09.05.16.
 */
public class IncompleteResult implements
  FilterFunction<Tuple3<CompressedDfsCode, Integer, Boolean>> {

  @Override
  public boolean filter(Tuple3<CompressedDfsCode, Integer, Boolean> triple
  ) throws
    Exception {

    return triple.f1 < 0;
  }
}
