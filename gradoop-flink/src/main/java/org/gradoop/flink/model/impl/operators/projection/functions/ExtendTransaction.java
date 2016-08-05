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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.projection.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.projection.common.BindingExtractor;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * Map Function to add additional vertices and edges defined by a production
 * pattern.
 */
public class ExtendTransaction
  extends RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * Production Pattern
   */
  private final String productionPattern;

  /**
   * QueryHandler for the Production Pattern
   */
  private QueryHandler productionHandler;

  /**
   * Factory for the vertices
   */
  private final VertexFactory vertexFactory;

  /**
   * Factory for the edges
   */
  private final EdgeFactory edgeFactory;

  /**
   * Extractor to get the binding Map of GraphElements
   */
  private final BindingExtractor extractor;

  /**
   * @param productionPattern ProductionPattern for QueryHandler
   * @param vertexFactory     Factory for vertices
   * @param edgeFactory       Factory for edges
   * @param extractor         extractor for variable bindings
   */
  public ExtendTransaction(String productionPattern,
    VertexFactory vertexFactory, EdgeFactory edgeFactory,
    BindingExtractor extractor) {
    this.productionPattern = productionPattern;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.extractor = extractor;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.productionHandler = new QueryHandler(productionPattern);
  }

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {

    Set<String> vertexVars = Sets.newHashSet(productionHandler.getVertexCache()
      .keySet());

    Map<String, Vertex> vertices = Maps.newHashMap();

    Set<GradoopId> boundVertexIds = Sets.newHashSet();

    for (Vertex v : transaction.getVertices()) {
      String binding = extractor.getBindings(v)
        .get(transaction.f0.getId().toString());


      if (vertexVars.remove(binding)) {
        boundVertexIds.add(v.getId());
        vertices.put(binding, v);
      }
    }

    for (String var : vertexVars) {
      org.s1ck.gdl.model.Vertex v = productionHandler.getVertexCache().get(var);
      vertices.put(var, vertexFactory.createVertex(v.getLabel(),
        PropertyList.createFromMap(v.getProperties()),
        GradoopIdSet.fromExisting(transaction.f0.getId())));
    }

    Set<String> edgeVars = Sets.newHashSet(productionHandler.getEdgeCache()
      .keySet());

    Set<Edge> edges = Sets.newHashSet();

    for (Edge e : transaction.getEdges()) {
      String binding = extractor.getBindings(e)
        .get(transaction.f0.getId().toString());

      if (edgeVars.remove(binding)) {
        if (boundVertexIds.contains(e.getSourceId()) &&
          boundVertexIds.contains(e.getTargetId())) {
          edges.add(e);
        } else {
          throw new IllegalStateException("All vertices of reused edges have " +
            "to be bound.");
        }
      }
    }

    for (String var : edgeVars) {
      org.s1ck.gdl.model.Edge e = productionHandler.getEdgeCache().get(var);

      String sourceVar = productionHandler.getVertexById(e.getSourceVertexId())
        .getVariable();
      String targetVar = productionHandler.getVertexById(e.getTargetVertexId())
        .getVariable();

      GradoopId sourceId = vertices.get(sourceVar).getId();
      GradoopId targetId = vertices.get(targetVar).getId();

      edges.add(edgeFactory.createEdge(e.getLabel(), sourceId, targetId,
        PropertyList.createFromMap(e.getProperties()),
        GradoopIdSet.fromExisting(transaction.f0.getId())));
    }

    return new GraphTransaction(transaction.f0,
      Sets.newHashSet(vertices.values()), edges);
  }
}
