package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.DebugOut;

/**
 * Created by peet on 09.05.16.
 */
public class IncompleteResult implements
  FilterFunction<Tuple3<CompressedDFSCode, Integer, Boolean>> {

  @Override
  public boolean filter(Tuple3<CompressedDFSCode, Integer, Boolean> triple
  ) throws
    Exception {

    if(triple.f1 < 0) {
      DebugOut.print("Inc", triple.f0);
    }

    return triple.f1 < 0;
  }
}
