package org.gradoop.flink.model.impl.operators.projection;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.gradoop.flink.model.impl.operators.projection.TestData.*;

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

  private final Class<? extends Exception> expectedException;

  public ProjectionTest(String testName, String matchResult,
    String productionGraph, String[] expectedGraphVariables,
    String expectedCollection, Class<? extends Exception> expectedException) {
    this.testName = testName;
    this.matchResult = matchResult;
    this.productionGraph = productionGraph;
    this.expectedGraphVariables = expectedGraphVariables;
    this.expectedCollection = expectedCollection;
    this.expectedException = expectedException;
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new Object[][]{
      {
        "NoMatchesPattern0", "", PATTERN_0, new String[] {}, "", null
      },
      {
        "SingleVertexPattern0", GRAPH_1, PATTERN_0,
        new String[] {"expected1"},
        "expected1[" +
        "(:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
        "]",
        null
      },
      {
        "TwoVerticesPattern2", GRAPH_2, PATTERN_2,
        new String[] {"expected1"},
        "expected1[" +
          "(:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
          "]",
        null
      },
      {
        "TwoVerticesOneEdgePattern3", GRAPH_3, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v2:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(v3:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
          "(v2)-[:married {id=0, bindings=\"1:c\"}]->(v3)" +
          "]",
        null
      },
      {
        "TwoVerticesPattern2", GRAPH_1, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v2:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(v3)" +
          "(v2)-->(v3)" +
          "]",
        null
      },
      {
        "FirstEdgeCases", GRAPH_5, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "()-->()" +
          "]",
        null
      },
      {
        "SecondEdgeCases", GRAPH_6, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "()-->(v1)" +
          "]",
        null
      },
      {
        "ThirdEdgeCases", GRAPH_7, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v0)-->()" +
          "]",
        null
      },
      {
        "FourthEdgeCases", GRAPH_8, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v0)-->(v1)" +
          "]",
        null
      },
      {
        "FifththEdgeCases", GRAPH_9, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "()-[e1]  ->()" +
          "]",
        IllegalStateException.class
      },
      {
        "SixthEdgeCases", GRAPH_10, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "()-[e1]->(v1)" +
          "]",
        IllegalStateException.class
      },
      {
        "SeventhEdgeCases", GRAPH_11, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v0)-[e1]->()" +
          "]",
        IllegalStateException.class
      },
      {
        "EightthEdgeCases", GRAPH_12, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v0)-[e1]->(v1)" +
          "]",
        null
      }
    });
  }

  @Test
  public void testGraphElementEquality() throws Exception {
    final String BINDINGS = "bindings";

    FlinkAsciiGraphLoader loader = getLoaderFromString(matchResult);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    Map<String, GradoopId> graphHeadIds = Maps.newHashMap();

    for (GraphHead gh : loader.getGraphHeads()) {
      PropertyValue id = gh.getPropertyValue("id");
      if (id != null) {
        graphHeadIds.put(String.valueOf(id.getInt()), gh.getId());
      }
    }

    Collection<GraphElement> elements = Lists.newArrayList();
    elements.addAll(loader.getVertices());
    elements.addAll(loader.getEdges());

    for (GraphElement e : elements) {
      String finalBinding = "";
      PropertyValue bindings = e.getProperties().get(BINDINGS);
      if (bindings != null) {
        for (String split : bindings.getString().split(",")) {
          finalBinding += graphHeadIds.get(split.split(":")[0]).toString();
          finalBinding += ":";
          finalBinding += split.split(":")[1];
          finalBinding += ",";
        }
        finalBinding = finalBinding.substring(0, finalBinding.length()-1);
        e.setProperty(BINDINGS, finalBinding);
      }
    }

    Projection projection = new Projection(
        loader.getGraphCollectionByVariables(TestData.DATA_GRAPH_VARIABLE),
        productionGraph, config, BINDINGS);

    try {
      // execute and validate
      collectAndAssertTrue(projection.execute(null).equalsByGraphElementData(
        loader.getGraphCollectionByVariables(expectedGraphVariables)));
      if (expectedException != null) {
        Assert.fail("The test should fail with: "
          + expectedException.getSimpleName());
      }
    } catch (Exception e) {
      Assert.assertEquals("The test should fail with: "
        + expectedException.getSimpleName(),
        expectedException, e.getCause().getClass());
    }
  }
}
