package stanford.infolab.debugger.instrumenter.examples;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import stanford.infolab.debugger.instrumenter.DebugConfig;

/**
 * Debug configuration file for SimpleShortestPathDebugComputation.
 * 
 * @author semihsalihoglu
 */
public class SimpleShortestPathsDebugConfig extends DebugConfig<
  LongWritable, DoubleWritable, FloatWritable, DoubleWritable, DoubleWritable>{

  // TODO(semih): This is work in progress. For now just debugging superstep 2.
  @Override
  public boolean shouldDebugSuperstep(long superstepNo) {
    return true;
  }

  // TODO(semih): This is work in progress. For now just debugging vertex 1.
  @Override
  public boolean shouldDebugVertex(LongWritable vertexId) {
    // vertexId.get() == 4L;
    return true;
  }
}
