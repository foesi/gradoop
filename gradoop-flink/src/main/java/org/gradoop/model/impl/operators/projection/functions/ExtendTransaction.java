package org.gradoop.model.impl.operators.projection.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.projection.common.BindingExtractor;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.Map;
import java.util.Set;

public class ExtendTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends RichMapFunction<GraphTransaction<G, V, E>, GraphTransaction<G, V,
  E>> {

  private final String productionPattern;
  private QueryHandler productionHandler;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;
  private final BindingExtractor extractor;

  public ExtendTransaction(String productionPattern,
    EPGMVertexFactory<V> vertexFactory, EPGMEdgeFactory<E> edgeFactory,
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
  public GraphTransaction<G, V, E> map(
    GraphTransaction<G, V, E> transaction) throws Exception {

    Set<String> vertexVars = Sets.newHashSet(productionHandler.getVertexCache()
      .keySet());
    
    Map<String, V> vertices = Maps.newHashMap();

    Set<GradoopId> boundVertexIds = Sets.newHashSet();

    for (V v : transaction.getVertices()) {
      String binding = extractor.getBindings(v)
        .get(transaction.f0.getId().toString());


      if (vertexVars.remove(binding)) {
        boundVertexIds.add(v.getId());
      }
      vertices.put(binding, v);
    }

    for (String var : vertexVars) {
      Vertex v = productionHandler.getVertexCache().get(var);
      vertices.put(var, vertexFactory.createVertex(v.getLabel(),
        PropertyList.createFromMap(v.getProperties()),
        GradoopIdSet.fromExisting(transaction.f0.getId())));
    }
    
    Set<String> edgeVars = Sets.newHashSet(productionHandler.getEdgeCache()
      .keySet());
    
    Set<E> edges = Sets.newHashSet();
    
    for (E e : transaction.getEdges()) {
      String binding = extractor.getBindings(e)
        .get(transaction.f0.getId().toString());

      if (!boundVertexIds.contains(e.getSourceId())
        && !boundVertexIds.contains(e.getTargetId())) {
        edgeVars.remove(binding);
        edges.add(e);
      }
    }
    
    for (String var : edgeVars) {
      Edge e = productionHandler.getEdgeCache().get(var);

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
    
    return new GraphTransaction<>(transaction.f0,
      Sets.newHashSet(vertices.values()), edges);
  }
}
