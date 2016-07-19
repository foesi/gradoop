package org.gradoop.model.impl.operators.projection;

public class TestData {

  public static final String DATA_GRAPH_VARIABLE = "db";

  public static final String PATTERN_0 = "(a)";

  public static final String PATTERN_1 = "";

  public static final String PATTERN_2 = "(a)(b)";

  public static final String PATTERN_3 = "(a)-c->(b)";

  public static final String GRAPH_1 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"g1:a\"})" +
    "]";

  public static final String GRAPH_2 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"g1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"g1:b\"})" +
    "]";

  public static final String GRAPH_3 = DATA_GRAPH_VARIABLE +
    "[" +
    "(v0:Person {id=0, sex=\"m\", bindings=\"g1:a\"})" +
    "(v1:Person {id=1, sex=\"w\", bindings=\"g1:b\"})" +
    "(v0)-[e1:married {id=0, bindings=\"g1:c\"}]->(v1)" +
    "]";

}
