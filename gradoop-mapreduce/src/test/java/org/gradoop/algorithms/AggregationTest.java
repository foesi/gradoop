package org.gradoop.algorithms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.GConstants;
import org.gradoop.HBaseClusterTest;
import org.gradoop.io.reader.AdjacencyListReader;
import org.gradoop.io.reader.EPGVertexReader;
import org.gradoop.model.Graph;
import org.gradoop.model.inmemory.MemoryGraph;
import org.gradoop.storage.GraphStore;
import org.gradoop.storage.hbase.EPGGraphHandler;
import org.gradoop.storage.hbase.EPGVertexHandler;
import org.gradoop.storage.hbase.GraphHandler;
import org.gradoop.storage.hbase.VertexHandler;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Simple test for reading EPG from HBase, process it via MapReduce
 * (Aggregate) and write the results back to HBase.
 */
public class AggregationTest extends HBaseClusterTest {

  private static final String VCOUNT_PROPERTY_KEY = "vcount";

  @Test
  public void aggregateTest()
    throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = utility.getConfiguration();
    GraphStore graphStore = createEmptyGraphStore();
    BufferedReader bufferedReader = createTestReader(EXTENDED_GRAPH);
    AdjacencyListReader adjacencyListReader =
      new AdjacencyListReader(graphStore, new EPGVertexReader());
    // store the graph
    adjacencyListReader.read(bufferedReader);

    // define MapReduce job
    Job job = new Job(conf, AggregationTest.class.getName());
    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    // map
    TableMapReduceUtil.initTableMapperJob(
      GConstants.DEFAULT_TABLE_VERTICES,
      scan,
      AggregateTableMapper.class,
      LongWritable.class,
      IntWritable.class,
      job
    );
    // reduce
    TableMapReduceUtil.initTableReducerJob(
      GConstants.DEFAULT_TABLE_GRAPHS,
      AggregateTableReducer.class,
      job
    );
    job.setNumReduceTasks(1);
    // run MR job
    job.waitForCompletion(true);

    // validate
    validateGraphs(graphStore);

    graphStore.close();
  }

  private void validateGraphs(GraphStore graphStore) {
    validateGraph(graphStore.readGraph(0L), 1, 2);
    validateGraph(graphStore.readGraph(1L), 1, 2);
  }

  private void validateGraph(Graph g, int expectedPropertyCount,
                             int expectedVertexCount) {
    assertEquals(expectedPropertyCount, g.getPropertyCount());
    assertEquals(expectedVertexCount, g.getProperty(VCOUNT_PROPERTY_KEY));
  }

  public static class AggregateTableMapper extends TableMapper<LongWritable,
    IntWritable> {

    private static VertexHandler VERTEX_HANDLER = new EPGVertexHandler();
    private static IntWritable ONE = new IntWritable(1);

    /**
     * Emits 1 for each graph that vertex is in.
     *
     * @param key     HBase row key
     * @param value   HBase row
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context)
      throws IOException, InterruptedException {
      for (Long graph : VERTEX_HANDLER.readGraphs(value)) {
        context.write(new LongWritable(graph), ONE);
      }
    }
  }

  public static class AggregateTableReducer extends
    TableReducer<LongWritable, IntWritable, ImmutableBytesWritable> {

    private static GraphHandler GRAPH_HANDLER = new EPGGraphHandler();

    /**
     * Counts all vertices inside a graph and stores it back to HBase.
     *
     * @param key     Graph Identifier
     * @param values  Contains a single one for each vertex inside the graph
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(LongWritable key, Iterable<IntWritable> values,
                          Context context)
      throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable ignored : values) {
        count++;
      }

      Graph g = new MemoryGraph(key.get());
      g.addProperty(VCOUNT_PROPERTY_KEY, count);

      Put put = new Put(Bytes.toBytes(key.get()));
      put = GRAPH_HANDLER.writeProperties(put, g);

      context.write(null, put);
    }
  }
}