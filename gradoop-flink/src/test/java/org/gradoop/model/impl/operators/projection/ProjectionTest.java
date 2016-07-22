package org.gradoop.model.impl.operators.projection;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.gradoop.model.impl.operators.projection.TestData.*;

/**
 * Base class for Pattern Matching Te2sts.
 */
@RunWith(Parameterized.class)
public class ProjectionTest extends GradoopFlinkTestBase {

  private final String testName;

  private final String matchResult;

  private final String productionGraph;

  private final String[] expectedGraphVariables;

  private final String expectedCollection;

  private final boolean positiveTest;

  public ProjectionTest(String testName, String matchResult,
    String productionGraph, String[] expectedGraphVariables,
    String expectedCollection, boolean positiveTest) {
    this.testName = testName;
    this.matchResult = matchResult;
    this.productionGraph = productionGraph;
    this.expectedGraphVariables = expectedGraphVariables;
    this.expectedCollection = expectedCollection;
    this.positiveTest = positiveTest;
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new Object[][]{
      {
        "NoMatchesPattern0", "", PATTERN_0, new String[] {}, "", true
      },
      {
        "SingleVertexPattern0", GRAPH_1, PATTERN_0,
        new String[] {"expected1"},
        "expected1[" +
        "(:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
        "]",
        true
      },
      {
        "SingleVertexNegPattern0", GRAPH_1, PATTERN_0,
        new String[] {"expected1"},
        "expected1[" +
          "(:Person {id=1, sex=\"m\", bindings=\"1:a\"})" +
          "]",
        false
      },
      {
        "TwoVerticesPattern2", GRAPH_2, PATTERN_2,
        new String[] {"expected1"},
        "expected1[" +
          "(:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
          "]",
        true
      },
      {
        "TwoVerticesPattern3", GRAPH_2, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
          "]",
        true
      },
      {
        "TwoVerticesOneEdgePattern3", GRAPH_3, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v2:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(v3:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
          "(v2)-[:married {id=0, bindings=\"1:c\"}]->(v3)" +
          "]",
        true
      },
      {
        "TwoVerticesPattern2", GRAPH_1, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v2:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(v3)" +
          "(v2)-->(v3)" +
          "]",
        true
      }
    });
  }

  @Test
  public void testGraphElementEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(matchResult);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    Projection<GraphHeadPojo, VertexPojo, EdgePojo> projection =
      new Projection<>(
        loader.getGraphCollectionByVariables(TestData.DATA_GRAPH_VARIABLE),
        productionGraph, config, "bindings");

    // execute and validate
    if (positiveTest) {
      collectAndAssertTrue(projection.execute(null)
        .equalsByGraphElementData(
          loader.getGraphCollectionByVariables(expectedGraphVariables)));
    } else {
      collectAndAssertFalse(projection.execute(null)
        .equalsByGraphElementData(
          loader.getGraphCollectionByVariables(expectedGraphVariables)));
    }
  }
}
