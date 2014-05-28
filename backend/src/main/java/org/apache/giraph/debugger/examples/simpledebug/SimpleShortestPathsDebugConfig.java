package org.apache.giraph.debugger.examples.simpledebug;

import org.apache.giraph.debugger.instrumenter.DebugConfig;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Debug configuration file for SimpleShortestPathDebugComputation.
 * 
 * @author semihsalihoglu
 */
public class SimpleShortestPathsDebugConfig extends DebugConfig<
  LongWritable, DoubleWritable, FloatWritable, DoubleWritable, DoubleWritable>{

  @Override
  public boolean shouldDebugSuperstep(long superstepNo) {
    return true;
  }

  @Override
  public boolean shouldDebugVertex(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex) {
    return true;
  }
}
