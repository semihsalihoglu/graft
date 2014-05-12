package stanford.infolab.debugger.examples.integrity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import stanford.infolab.debugger.instrumenter.DebugConfig;

/**
 * Debug configuration file for ConnectedComponents, that is configured to check the integrity
 * of the vertex values: The current check is that the vertex value is less than or equal to the
 * id of the vertex.
 * 
 * @author semihsalihoglu
 */
public class CopyOfConnectedComponentsVValueIntegrityDebugConfig extends DebugConfig<IntWritable,
  IntWritable, NullWritable, IntWritable, IntWritable> {

  @Override
  public boolean shouldCheckVertexValueIntegrity() {
    return true;
  }
  
  @Override
  public boolean isVertexValueCorrect(IntWritable vertexId, IntWritable value) {
    return false; //value.get() < vertexId.get();
  }
}
