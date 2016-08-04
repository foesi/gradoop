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

public class ExtendTransaction
  extends RichMapFunction<GraphTransaction, GraphTransaction> {

  private final String productionPattern;
  private QueryHandler productionHandler;
  private final VertexFactory vertexFactory;
  private final EdgeFactory edgeFactory;
  private final BindingExtractor extractor;

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

      if (boundVertexIds.contains(e.getSourceId())
        && boundVertexIds.contains(e.getTargetId())) {
        edgeVars.remove(binding);
        edges.add(e);
      }
    }
    
    for (String var : edgeVars) {
      org.s1ck.gdl.model.Edge e = productionHandler.getEdgeCache().get(var);

      String sourceVar = productionHandler.getVertexById(e.getSourceVertexId())
        .getVariable();
      String targetVar = productionHandler.getVertexById(e.getTargetVertexId())
        .getVariable();

      if (vertexVars.contains(sourceVar) || vertexVars.contains(targetVar)) {
        GradoopId sourceId = vertices.get(sourceVar).getId();
        GradoopId targetId = vertices.get(targetVar).getId();

        edges.add(edgeFactory.createEdge(e.getLabel(), sourceId, targetId,
          PropertyList.createFromMap(e.getProperties()),
          GradoopIdSet.fromExisting(transaction.f0.getId())));
      }
    }
    
    return new GraphTransaction(transaction.f0,
      Sets.newHashSet(vertices.values()), edges);
  }
}
