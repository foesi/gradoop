package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.Collection;
import java.util.Map;

/**
 * Created by peet on 09.05.16.
 */
public class FrequentOrRefinementCandidate
  extends RichGroupReduceFunction
  <Tuple3<CompressedDFSCode, Integer, Boolean>,
    Tuple3<CompressedDFSCode, Integer, Boolean>> {

  private final float threshold;
  /**
   * minimum support
   */
  private Integer minSupport;
  private Map<Integer, Integer> workerGraphCount;

  public FrequentOrRefinementCandidate(FSMConfig fsmConfig) {
    this.threshold = fsmConfig.getThreshold();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minSupport = getRuntimeContext()
      .<Integer>getBroadcastVariable(
        BroadcastNames.MIN_SUPPORT).get(0);

    this.workerGraphCount = getRuntimeContext()
      .<Map<Integer, Integer>>getBroadcastVariable(
        BroadcastNames.WORKER_GRAPHCOUNT).get(0);
  }

  @Override
  public void reduce(
    Iterable<Tuple3<CompressedDFSCode, Integer, Boolean>> iterable,
    Collector<Tuple3<CompressedDFSCode, Integer, Boolean>> collector) throws
    Exception {

    // copy list of all workers
    Collection<Integer> workerIdsWithoutReport = Sets
      .newHashSet(workerGraphCount.keySet());

    // init aggregate variables
    boolean first = true;
    CompressedDFSCode code = null;
    int support = 0;
    boolean altLeastOnceLocallyFrequent = false;

    // for each worker report
    for(Tuple3<CompressedDFSCode, Integer, Boolean> triple : iterable) {
      if(first) {
        code = triple.f0;
        first = false;
      }

      Integer reportedWorkerId = triple.f1;
      Boolean locallyFrequent = triple.f2;
      support += code.getSupport();

      if(!altLeastOnceLocallyFrequent && locallyFrequent) {
        altLeastOnceLocallyFrequent = true;
      }

      workerIdsWithoutReport.remove(reportedWorkerId);
    }

    // CANDIDATE SELECTION

    if(altLeastOnceLocallyFrequent) {
      // remember known support
      code.setSupport(support);

      // support of all workers known
      if(workerIdsWithoutReport.isEmpty()) {
        // if globally frequent
        if(support >= minSupport) {
          // emit complete support message
          collector.collect(new Tuple3<>(code, -1, true));
        }
      } else {
        int estimation = support;

        // add optimistic support estimations
        for(Integer workerId : workerIdsWithoutReport) {
          estimation += (workerGraphCount.get(workerId) * threshold);
        }
        // if likely globally frequent
        if(estimation >= minSupport) {
          // emit incomplete support message
          collector.collect(new Tuple3<>(code, -1, false));

          // add refinement calls to missing workers
          for(Integer workerId : workerIdsWithoutReport) {
            code.setSupport(0);
            collector.collect(new Tuple3<>(code, workerId, false));
          }
        }
      }
    }
  }
}