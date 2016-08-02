package org.gradoop.flink.model.impl.operators.projection;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
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


  public ProjectionTest(String testName, String matchResult,
    String productionGraph, String[] expectedGraphVariables,
    this.testName = testName;
    this.matchResult = matchResult;
    this.productionGraph = productionGraph;
    this.expectedGraphVariables = expectedGraphVariables;
    this.expectedCollection = expectedCollection;
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new Object[][]{
      {
        "NoMatchesPattern0", "", PATTERN_0, new String[] {}, ""
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
          "]" 
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
          "]" 
      },
      {
        "TwoVerticesOneEdgePattern3", GRAPH_3, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v2:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(v3:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
          "(v2)-[:married {id=0, bindings=\"1:c\"}]->(v3)" +
          "]"
      },
      {
        "TwoVerticesPattern2", GRAPH_1, PATTERN_3,
        new String[] {"expected1"},
        "expected1[" +
          "(v2:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
          "(v3)" +
          "(v2)-->(v3)" +
          "]"
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

    // execute and validate
    collectAndAssertTrue(projection.execute(null).equalsByGraphElementData(
      loader.getGraphCollectionByVariables(expectedGraphVariables)));
  }
}
