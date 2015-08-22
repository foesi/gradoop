/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.summarization;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.GConstants;
import org.gradoop.model.EdgeData;
import org.gradoop.model.EdgeDataFactory;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

/**
 * The summarization operator determines a structural grouping of similar
 * vertices and edges to condense a graph and thus help to uncover insights
 * about patterns hidden in the graph.
 * <p>
 * The graph summarization operator represents every vertex group by a single
 * vertex in the summarized graph; edges between vertices in the summary graph
 * represent a group of edges between the vertex group members of the
 * original graph. Summarization is defined by specifying grouping keys for
 * vertices and edges, respectively, similarly as for GROUP BY in SQL.
 * <p>
 * Consider the following example:
 * <p>
 * Input graph:
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L})<br>
 * (1, "Person", {city: L})<br>
 * (2, "Person", {city: D})<br>
 * (3, "Person", {city: D})<br>
 * <p>
 * Edges:{(0,1), (1,0), (1,2), (2,1), (2,3), (3,2)}
 * <p>
 * Output graph (summarized on vertex property "city"):
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L, count: 2})
 * (2, "Person", {city: D, count: 2})
 * <p>
 * Edges:<br>
 * ((0, 0), {count: 2}) // 2 intra-edges in L<br>
 * ((2, 2), {count: 2}) // 2 intra-edges in L<br>
 * ((0, 2), {count: 1}) // 1 inter-edge from L to D<br>
 * ((2, 0), {count: 1}) // 1 inter-edge from D to L<br>
 * <p>
 * In addition to vertex properties, summarization is also possible on edge
 * properties, vertex- and edge labels as well as combinations of those.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public abstract class Summarization<VD extends VertexData, ED extends
  EdgeData, GD extends GraphData> implements
  UnaryGraphToGraphOperator<VD, ED, GD> {
  /**
   * Used to represent vertices that do not have the vertex grouping property.
   */
  public static final String NULL_VALUE = "__NULL";
  /**
   * Property key to store the number of summarized entities in a group.
   */
  protected static final String COUNT_PROPERTY_KEY = "count";
  /**
   * Creates new graph data objects.
   */
  protected GraphDataFactory<GD> graphDataFactory;
  /**
   * Creates new vertex data objects.
   */
  protected VertexDataFactory<VD> vertexDataFactory;
  /**
   * Creates new edge data objects.
   */
  protected EdgeDataFactory<ED> edgeDataFactory;
  /**
   * Used to summarize vertices.
   */
  private final String vertexGroupingKey;
  /**
   * Used to summarize edges.
   */
  private final String edgeGroupingKey;
  /**
   * True if vertices shall be summarized using their label.
   */
  private final boolean useVertexLabels;
  /**
   * True if edges shall be summarized using their label.
   */
  private final boolean useEdgeLabels;

  /**
   * Creates summarization.
   *
   * @param vertexGroupingKey property key to summarize vertices
   * @param edgeGroupingKey   property key to summarize edges
   * @param useVertexLabels   summarize on vertex label true/false
   * @param useEdgeLabels     summarize on edge label true/false
   */
  Summarization(String vertexGroupingKey, String edgeGroupingKey,
    boolean useVertexLabels, boolean useEdgeLabels) {
    this.vertexGroupingKey = vertexGroupingKey;
    this.edgeGroupingKey = edgeGroupingKey;
    this.useVertexLabels = useVertexLabels;
    this.useEdgeLabels = useEdgeLabels;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> graph) {
    LogicalGraph<VD, ED, GD> result;
    Graph<Long, VD, ED> gellyGraph;

    vertexDataFactory = graph.getVertexDataFactory();
    edgeDataFactory = graph.getEdgeDataFactory();
    graphDataFactory = graph.getGraphDataFactory();

    if (!useVertexProperty() &&
      !useEdgeProperty() && !useVertexLabels() && !useEdgeLabels()) {
      // graphs stays unchanged
      result = graph;
    } else {
      GD graphData = createNewGraphData();
      gellyGraph = summarizeInternal(graph.getGellyGraph());
      result = LogicalGraph
        .fromGraph(gellyGraph, graphData, graph.getVertexDataFactory(),
          graph.getEdgeDataFactory(), graph.getGraphDataFactory());
    }
    return result;
  }

  /**
   * Returns true if the vertex property shall be used for summarization.
   *
   * @return true if vertex property shall be used for summarization, false
   * otherwise
   */
  protected boolean useVertexProperty() {
    return vertexGroupingKey != null && !"".equals(vertexGroupingKey);
  }

  /**
   * Vertex property key to use for summarizing vertices.
   *
   * @return vertex property key
   */
  protected String getVertexGroupingKey() {
    return vertexGroupingKey;
  }

  /**
   * True, if vertex labels shall be used for summarization.
   *
   * @return true, if vertex labels shall be used for summarization, false
   * otherwise
   */
  protected boolean useVertexLabels() {
    return useVertexLabels;
  }

  /**
   * Returns true if the edge property shall be used for summarization.
   *
   * @return true if edge property shall be used for summarization, false
   * otherwise
   */
  protected boolean useEdgeProperty() {
    return edgeGroupingKey != null && !"".equals(edgeGroupingKey);
  }

  /**
   * Edge property key to use for summarizing edges.
   *
   * @return edge property key
   */
  protected String getEdgeGroupingKey() {
    return edgeGroupingKey;
  }

  /**
   * True, if edge labels shall be used for summarization.
   *
   * @return true, if edge labels shall be used for summarization, false
   * otherwise
   */
  protected boolean useEdgeLabels() {
    return useEdgeLabels;
  }

  /**
   * Groups vertices by vertex label and/or grouping key.
   *
   * @param graph input graph
   * @return grouped vertices
   */
  protected UnsortedGrouping<Vertex<Long, VD>> groupVertices(
    Graph<Long, VD, ED> graph) {
    return graph.getVertices()
      // group vertices by the given property
      .groupBy(new VertexGroupingValueSelector<VD>(getVertexGroupingKey(),
        useVertexLabels()));
  }

  /**
   * Constructs new summarized vertices representing a group of vertices.
   *
   * @param groupedSortedVertices grouped and sorted vertices
   * @return summarized vertices
   */
  protected DataSet<Vertex<Long, VD>> buildSummarizedVertices(
    SortedGrouping<Vertex<Long, VD>> groupedSortedVertices) {
    return groupedSortedVertices.reduceGroup(
      new VertexGroupSummarizer<>(getVertexGroupingKey(), useVertexLabels(),
        vertexDataFactory));
  }

  /**
   * Build summarized edges by joining them with vertices and their group
   * representative.
   *
   * @param graph                     inout graph
   * @param vertexToRepresentativeMap dataset containing tuples of vertex id
   *                                  and group representative
   * @return summarized edges
   */
  protected DataSet<Edge<Long, ED>> buildSummarizedEdges(
    Graph<Long, VD, ED> graph,
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap) {
    // join edges with vertex-group-map on vertex-id == edge-source-id
    DataSet<EdgeGroupItem> edges =
      graph.getEdges().join(vertexToRepresentativeMap).where(0).equalTo(0)
        // project edges to necessary information
        .with(new SourceVertexJoinFunction<ED>(getEdgeGroupingKey(),
          useEdgeLabels()))
          // join result with vertex-group-map on edge-target-id == vertex-id
        .join(vertexToRepresentativeMap).where(2).equalTo(0)
        .with(new TargetVertexJoinFunction());

    return groupEdges(edges).reduceGroup(
      new EdgeGroupSummarizer<>(getEdgeGroupingKey(), useEdgeLabels(),
        edgeDataFactory)).withForwardedFields("f0");
  }

  /**
   * Groups edges based on the algorithm parameters.
   *
   * @param edges input graph edges
   * @return grouped edges
   */
  protected UnsortedGrouping<EdgeGroupItem> groupEdges(
    DataSet<EdgeGroupItem> edges) {
    UnsortedGrouping<EdgeGroupItem> groupedEdges;
    if (useEdgeProperty() && useEdgeLabels()) {
      groupedEdges = edges.groupBy(1, 2, 3, 4);
    } else if (useEdgeLabels()) {
      groupedEdges = edges.groupBy(1, 2, 3);
    } else if (useEdgeProperty()) {
      groupedEdges = edges.groupBy(1, 2, 4);
    } else {
      groupedEdges = edges.groupBy(1, 2);
    }
    return groupedEdges;
  }

  /**
   * Creates new graph data for the resulting logical graph.
   *
   * @return graph data
   */
  private GD createNewGraphData() {
    return graphDataFactory.createGraphData(FlinkConstants.SUMMARIZE_GRAPH_ID);
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graph
   * @return summarized output graph
   */
  protected abstract Graph<Long, VD, ED> summarizeInternal(
    Graph<Long, VD, ED> graph);

  /**
   * Selects the property value to group vertices. If grouping on property
   * and label is requested, the selector returns a concatenated string value
   * build from label and property value.
   */
  protected static class VertexGroupingValueSelector<VD extends VertexData>
    implements
    KeySelector<Vertex<Long, VD>, Integer> {
    /**
     * Defines how label and grouping value are represented.
     */
    private static final String GROUPING_VALUE_FORMAT = "%s%s";
    /**
     * Vertex property key
     */
    private final String groupPropertyKey;
    /**
     * True, if property shall be considered.
     */
    private final boolean useProperty;
    /**
     * True, if label shall be considered
     */
    private final boolean useLabel;

    /**
     * Creates key selector
     *
     * @param groupPropertyKey vertex property key
     * @param useLabel         use vertex label
     */
    public VertexGroupingValueSelector(String groupPropertyKey,
      boolean useLabel) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getKey(Vertex<Long, VD> v) throws Exception {
      String label = v.getValue().getLabel();
      String groupingValue = null;
      boolean hasProperty =
        useProperty && (v.getValue().getProperty(groupPropertyKey) != null);

      if (useLabel && useProperty && hasProperty) {
        groupingValue = String.format(GROUPING_VALUE_FORMAT, label,
          v.getValue().getProperty(groupPropertyKey).toString());
      } else if (useLabel && useProperty) {
        groupingValue = String.format(GROUPING_VALUE_FORMAT, label, NULL_VALUE);
      } else if (useLabel) {
        groupingValue = label;
      } else if (useProperty && hasProperty) {
        groupingValue = v.getValue().getProperty(groupPropertyKey).toString();
      } else if (useProperty) {
        groupingValue = NULL_VALUE;
      }
      assert groupingValue != null;
      return groupingValue.hashCode();
    }
  }

  /**
   * Creates a summarized vertex from a group of vertices.
   */
  protected static class VertexGroupSummarizer<VD extends VertexData> implements
    GroupReduceFunction<Vertex<Long, VD>, Vertex<Long, VD>>,
    ResultTypeQueryable<Vertex<Long, VD>> {

    /**
     * Vertex data factory
     */
    private final VertexDataFactory<VD> vertexDataFactory;
    /**
     * Vertex property key to store group value
     */
    private final String groupPropertyKey;
    /**
     * True, if label shall be considered
     */
    private final boolean useLabel;
    /**
     * True, if property shall be considered.
     */
    private final boolean useProperty;
    /**
     * Avoid object instantiation in each reduce call.
     */
    private final Vertex<Long, VD> reuseVertex;

    /**
     * Creates group reducer
     *
     * @param groupPropertyKey  vertex property key to store group value
     * @param useLabel          use vertex label
     * @param vertexDataFactory vertex data factory
     */
    public VertexGroupSummarizer(String groupPropertyKey, boolean useLabel,
      VertexDataFactory<VD> vertexDataFactory) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
      this.vertexDataFactory = vertexDataFactory;
      this.reuseVertex = new Vertex<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<Vertex<Long, VD>> vertices,
      Collector<Vertex<Long, VD>> collector) throws Exception {
      long groupCount = 0L;
      Long newVertexID = 0L;
      String groupLabel = null;
      String groupValue = null;
      boolean initialized = false;
      for (Vertex<Long, VD> v : vertices) {
        groupCount++;
        if (!initialized) {
          // will be the minimum vertex id in the group
          newVertexID = v.getId();
          // get label if necessary
          groupLabel = useLabel ? v.getValue().getLabel() :
            GConstants.DEFAULT_VERTEX_LABEL;
          // get group value if necessary
          if (useProperty) {
            groupValue = getGroupProperty(v.getValue());
          }
          initialized = true;
        }
      }
      VD vertexData =
        vertexDataFactory.createVertexData(newVertexID, groupLabel);
      if (useProperty) {
        vertexData.setProperty(groupPropertyKey, groupValue);
      }
      vertexData.setProperty(COUNT_PROPERTY_KEY, groupCount);
      vertexData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);

      reuseVertex.f0 = newVertexID;
      reuseVertex.f1 = vertexData;

      collector.collect(reuseVertex);
    }

    /**
     * Returns the group property value or the default value if vertices in
     * the group do not have the property.
     *
     * @param vertexData vertex data object of the summarized vertex
     * @return vertex group value
     */
    private String getGroupProperty(VD vertexData) {
      if (vertexData.getProperty(groupPropertyKey) != null) {
        return vertexData.getProperty(groupPropertyKey).toString();
      } else {
        return NULL_VALUE;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Vertex<Long, VD>> getProducedType() {
      return new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(vertexDataFactory.getType()));
    }
  }

  /**
   * Creates a summarized edge from a group of edges including an edge
   * grouping value.
   */
  protected static class EdgeGroupSummarizer<ED extends EdgeData> implements
    GroupReduceFunction<EdgeGroupItem, Edge<Long, ED>>,
    ResultTypeQueryable<Edge<Long, ED>> {

    /**
     * Edge data factory
     */
    private final EdgeDataFactory<ED> edgeDataFactory;
    /**
     * Edge property key to store group value
     */
    private final String groupPropertyKey;
    /**
     * True, if label shall be considered
     */
    private boolean useLabel;

    /**
     * True, if property shall be considered.
     */
    private boolean useProperty;
    /**
     * Avoid object instantiation in each reduce call.
     */
    private final Edge<Long, ED> reuseEdge;

    /**
     * Creates group reducer
     *
     * @param groupPropertyKey edge property key to store group value
     * @param useLabel         use edge label
     * @param edgeDataFactory  edge data factory
     */
    public EdgeGroupSummarizer(String groupPropertyKey, boolean useLabel,
      EdgeDataFactory<ED> edgeDataFactory) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
      this.edgeDataFactory = edgeDataFactory;
      this.reuseEdge = new Edge<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Iterable<EdgeGroupItem> edgeGroupItems,
      Collector<Edge<Long, ED>> collector) throws Exception {
      int edgeCount = 0;
      boolean initialized = false;
      // new edge id will be the first edge id in the group (which is sorted)
      Long newEdgeID = null;
      Long newSourceVertexId = null;
      Long newTargetVertexId = null;
      String edgeLabel = GConstants.DEFAULT_EDGE_LABEL;
      String edgeGroupingValue = null;

      for (EdgeGroupItem e : edgeGroupItems) {
        edgeCount++;
        if (!initialized) {
          newEdgeID = e.getEdgeId();
          newSourceVertexId = e.getSourceVertexId();
          newTargetVertexId = e.getTargetVertexId();
          if (useLabel) {
            edgeLabel = e.getGroupLabel();
          }
          edgeGroupingValue = e.getGroupPropertyValue();
          initialized = true;
        }
      }

      ED newEdgeData = edgeDataFactory
        .createEdgeData(newEdgeID, edgeLabel, newSourceVertexId,
          newTargetVertexId);

      if (useProperty) {
        newEdgeData.setProperty(groupPropertyKey, edgeGroupingValue);
      }
      newEdgeData.setProperty(COUNT_PROPERTY_KEY, edgeCount);
      newEdgeData.addGraph(FlinkConstants.SUMMARIZE_GRAPH_ID);

      reuseEdge.setSource(newSourceVertexId);
      reuseEdge.setTarget(newTargetVertexId);
      reuseEdge.setValue(newEdgeData);
      collector.collect(reuseEdge);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Edge<Long, ED>> getProducedType() {
      return new TupleTypeInfo(Edge.class, BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        TypeExtractor.createTypeInfo(edgeDataFactory.getType()));
    }
  }

  /**
   * Takes an edge and a tuple (vertex-id, group-representative) as input.
   * Replaces the edge-source-id with the group-representative and outputs
   * projected edge information possibly containing the edge label and a
   * group property.
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f1->f2") // edge target id
  @FunctionAnnotation.ForwardedFieldsSecond("f1") // edge source id
  protected static class SourceVertexJoinFunction<ED extends EdgeData>
    implements
    JoinFunction<Edge<Long, ED>, VertexWithRepresentative, EdgeGroupItem> {

    /**
     * Vertex property key for grouping
     */
    private final String groupPropertyKey;
    /**
     * True, if vertex label shall be considered.
     */
    private final boolean useLabel;
    /**
     * True, if vertex property shall be considered.
     */
    private final boolean useProperty;

    /**
     * Avoid object initialization in each call.
     */
    private final EdgeGroupItem reuseEdgeGroupItem;

    /**
     * Creates join function.
     *
     * @param groupPropertyKey vertex property key for grouping
     * @param useLabel         true, if vertex label shall be used
     */
    public SourceVertexJoinFunction(String groupPropertyKey, boolean useLabel) {
      this.groupPropertyKey = groupPropertyKey;
      this.useLabel = useLabel;
      this.reuseEdgeGroupItem = new EdgeGroupItem();
      this.useProperty =
        groupPropertyKey != null && !"".equals(groupPropertyKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EdgeGroupItem join(Edge<Long, ED> e,
      VertexWithRepresentative vertexRepresentative) throws Exception {
      String groupLabel = useLabel ? e.getValue().getLabel() : null;
      String groupPropertyValue = null;

      boolean hasProperty =
        useProperty && (e.getValue().getProperty(groupPropertyKey) != null);
      if (useProperty && hasProperty) {
        groupPropertyValue =
          e.getValue().getProperty(groupPropertyKey).toString();
      } else if (useProperty) {
        groupPropertyValue = NULL_VALUE;
      }
      reuseEdgeGroupItem.setEdgeId(e.getValue().getId());
      reuseEdgeGroupItem.setSourceVertexId(
        vertexRepresentative.getGroupRepresentativeVertexId());
      reuseEdgeGroupItem.setTargetVertexId(e.getTarget());
      reuseEdgeGroupItem.setGroupLabel(groupLabel);
      reuseEdgeGroupItem.setGroupPropertyValue(groupPropertyValue);

      return reuseEdgeGroupItem;
    }
  }

  /**
   * Takes a projected edge and an (vertex-id, group-representative) tuple
   * and replaces the edge-target-id with the group-representative.
   */
  @FunctionAnnotation.ForwardedFieldsFirst("f0;f1;f3;f4")
  @FunctionAnnotation.ForwardedFieldsSecond("f1->f2")
  protected static class TargetVertexJoinFunction implements
    JoinFunction<EdgeGroupItem, VertexWithRepresentative, EdgeGroupItem> {

    /**
     * {@inheritDoc}
     */
    @Override
    public EdgeGroupItem join(EdgeGroupItem edge,
      VertexWithRepresentative vertexRepresentative) throws Exception {
      edge.setField(vertexRepresentative.getGroupRepresentativeVertexId(), 2);
      return edge;
    }
  }
}
