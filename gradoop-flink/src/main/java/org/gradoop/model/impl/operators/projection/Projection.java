package org.gradoop.model.impl.operators.projection;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative
  .ExplorativeSubgraphIsomorphism;
import org.gradoop.model.impl.operators.projection.common.BindingExtractor;
import org.gradoop.model.impl.operators.projection.functions.ExtendTransaction;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

/**
  * Algorithm for mutating graphs with a given matching and production pattern.
  *
  * @param <G> EPGM graph head type
  * @param <V> EPGM vertex type
  * @param <E> EPGM edge type
  *
  */
public class Projection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E> {

  /**
   * GDL based query string.)
   */
  private final String query;

  /**
   * GDL based production string.
   */
  private final String production;

  /**
   *  Logger.
   */
  private final Logger log;

  /**
   * QueryHandler for the GDL production pattern.
   */
  private final QueryHandler patternHandler;


  /**
   * GradoopFlinkConfig.
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Extractor object for getting bindings from the elements of matching result.
   */
  private final BindingExtractor extractor;

  private GraphCollection<G, V, E> matchResult = null;

  /**
   * @param queryGraph        GDL queryGraph Graph
   * @param productionPattern GDL production Pattern
   * @param config            Gradoop Config
   * @param bindingsString    Property name where to find the variable bindings
   */
  public Projection(final String queryGraph, final String productionPattern,
    GradoopFlinkConfig<G, V, E> config, String bindingsString)
  {
    Preconditions.checkState(!Strings.isNullOrEmpty(queryGraph),
      "Query must not be null or empty");
    Preconditions.checkState(!Strings.isNullOrEmpty(productionPattern),
      "Production pattern must not be null or empty");
    this.query = queryGraph;
    this.production = productionPattern;
    this.log = Logger.getLogger(Projection.class);
    this.patternHandler = new QueryHandler(production);
    this.config = config;
    this.extractor = new BindingExtractor(bindingsString);
  }

  protected Projection(final GraphCollection<G, V, E> matchResult,
    final String productionPattern,
    GradoopFlinkConfig<G, V, E> config, String bindingsString) {
    this("no_pattern", productionPattern, config, bindingsString);
    this.matchResult = matchResult;
  }

  @Override
  public final GraphCollection<G, V, E> execute(
    final LogicalGraph<G, V, E> graph) {

    GraphCollection<G, V, E> matchesCol;

    if (matchResult == null) {
      PatternMatching<G, V, E> matcher =
        new ExplorativeSubgraphIsomorphism<>(this.query, true);

      matchesCol = matcher.execute(graph);
    } else {
      matchesCol = matchResult;
    }

    DataSet<GraphTransaction<G, V, E>> matchTrans = matchesCol
      .toTransactions().getTransactions();

    DataSet<GraphTransaction<G, V, E>> transactions = matchTrans
      .map(new ExtendTransaction<G, V, E>(production, config.getVertexFactory(),
        config.getEdgeFactory(), extractor));


    return GraphCollection.fromTransactions(
      new GraphTransactions<>(transactions, config));
  }

  @Override
  public final String getName() {
      return Projection.class.getName();
  }
}
