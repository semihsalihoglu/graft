package org.apache.giraph.debugger.examples.integrity;

import org.apache.giraph.debugger.DebugConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Debug configuration file for ConnectedComponents, that is configured to check the integrity
 * of the messages setn: The current check is that the message value is less than or equal to the
 * id of the source vertex.
 * 
 * @author semihsalihoglu
 */
public class ConnectedComponentsMsgIntegrityDebugConfig extends DebugConfig<IntWritable, IntWritable,
  NullWritable, IntWritable, IntWritable> {

  @Override
  public boolean shouldCheckMessageIntegrity() {
    return true;
  }
  
  @Override
  public boolean isMessageCorrect(IntWritable srcId, IntWritable dstId, IntWritable message) {
    return message.get() <= srcId.get();
  }
}
