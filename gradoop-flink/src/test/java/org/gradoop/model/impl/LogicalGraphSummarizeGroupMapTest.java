package org.gradoop.model.impl;

import org.gradoop.model.impl.operators.summarization.Summarization;
import org.gradoop.model.impl.operators.summarization.SummarizationGroupMap;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class LogicalGraphSummarizeGroupMapTest extends
  LogicalGraphSummarizeTestBase {

  public LogicalGraphSummarizeGroupMapTest(TestExecutionMode mode) {
    super(mode);
  }

  @Override
  public Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
  getSummarizationImpl(
    String vertexGroupingKey, boolean useVertexLabel, String edgeGroupingKey,
    boolean useEdgeLabel) {
    return new SummarizationGroupMap<>(vertexGroupingKey, edgeGroupingKey,
      useVertexLabel, useEdgeLabel);
  }
}