package org.gradoop.flink.model.impl.operators.projection;

public class TestData {

  public static final String DATA_GRAPH_VARIABLE = "db";

  public static final String PATTERN_0 = "(a)";

  public static final String PATTERN_1 = "";

  public static final String PATTERN_2 = "(a)(b)";

  public static final String PATTERN_3 = "(a)-[c]->(b)";

  public static final String GRAPH_1 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "]";

  public static final String GRAPH_2 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
    "]";

  public static final String GRAPH_3 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:c\"}]->(v1)" +
    "]";

  /*
        For Edges and their source and target Vertex there are the following
        possibilities for their binding in the production pattern. It can be
        either u for unbound or b for bound.
           |E|S|T
          -------
          0|u|u|u
          -------
          1|u|u|b
          -------
          2|u|b|u
          -------
          3|u|b|b
          -------
          4|b|u|u
          -------
          5|b|u|b
          -------
          6|b|b|u
          -------
          7|b|b|b
       */


  public static final String GRAPH_5 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:d\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:e\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:f\"}]->(v1)" +
    "]";

  public static final String GRAPH_6 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:d\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:f\"}]->(v1)" +
    "]";

  public static final String GRAPH_7 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:d\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:f\"}]->(v1)" +
    "]";

  public static final String GRAPH_8 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:f\"}]->(v1)" +
    "]";

  public static final String GRAPH_9 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:d\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:e\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:c\"}]->(v1)" +
    "]";

  public static final String GRAPH_10 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:d\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:c\"}]->(v1)" +
    "]";

  public static final String GRAPH_11 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:e\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:c\"}]->(v1)" +
    "]";

  public static final String GRAPH_12 = DATA_GRAPH_VARIABLE +
    " {id=1}[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"1:b\"})" +
    "(v0)-[e1:married {id=0, bindings=\"1:c\"}]->(v1)" +
    "]";
}
