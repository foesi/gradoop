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

package org.gradoop.flink.model.impl.operators.projection;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.flink.api.java.DataSet;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.query
  .QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.isomorphism
  .explorative.ExplorativeSubgraphIsomorphism;
import org.gradoop.flink.model.impl.operators.projection.common
  .BindingExtractor;
import org.gradoop.flink.model.impl.operators.projection.functions.BoundEdges;
import org.gradoop.flink.model.impl.operators.projection.functions
  .BoundVertices;
import org.gradoop.flink.model.impl.operators.projection.functions
  .ExtendTransaction;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
  * Algorithm for mutating graphs with a given matching and production pattern.
  *
  */
public class Projection
  implements UnaryGraphToCollectionOperator {

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
  private final GradoopFlinkConfig config;

  /**
   * Extractor object for getting bindings from the elements of matching result.
   */
  private final BindingExtractor extractor;

  /**
   * The match result which can be passed from tests to skip the whole
   * matching execution.
   */
  private GraphCollection matchResult = null;

  /**
   * @param queryGraph        GDL queryGraph Graph
   * @param productionPattern GDL production Pattern
   * @param config            Gradoop Config
   * @param bindingsString    Property name where to find the variable bindings
   */
  public Projection(final String queryGraph, final String productionPattern,
    GradoopFlinkConfig config, String bindingsString) {
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

  /**
   * This Constructor is used to bypass the matching operator for tests.
   *
   * @param matchResult       Predefined result of matching operator
   * @param productionPattern GDL production Pattern
   * @param config            Gradoop Config
   * @param bindingsString    Property name where to find the variable bindings
   */
  protected Projection(final GraphCollection matchResult,
    final String productionPattern,
    GradoopFlinkConfig config, String bindingsString) {
    this("no_pattern", productionPattern, config, bindingsString);
    this.matchResult = matchResult;
  }

  @Override
  public final GraphCollection execute(final LogicalGraph graph) {

    GraphCollection matchesCol;

    if (matchResult == null) {
      PatternMatching matcher =
        new ExplorativeSubgraphIsomorphism(this.query, true);

      matchesCol = matcher.execute(graph);
    } else {
      matchesCol = matchResult;
    }

    // filter unbound vertices and edges so only bound ones are forwarded to
    // the toTransaction method
    DataSet<Vertex> boundVertices = matchesCol.getVertices()
      .filter(new BoundVertices(production));

    DataSet<Edge> boundEdges = matchesCol.getEdges()
      .filter(new BoundEdges(production));

    GraphCollection boundMatches = GraphCollection.fromDataSets(matchesCol
      .getGraphHeads(), boundVertices, boundEdges, config);

    DataSet<GraphTransaction> matchTrans = boundMatches
      .toTransactions().getTransactions();

    DataSet<GraphTransaction> transactions = matchTrans
      .map(new ExtendTransaction(production, config.getVertexFactory(),
        config.getEdgeFactory(), extractor));


    return GraphCollection.fromTransactions(
      new GraphTransactions(transactions, config));
  }

  @Override
  public final String getName() {
    return Projection.class.getName();
  }
}