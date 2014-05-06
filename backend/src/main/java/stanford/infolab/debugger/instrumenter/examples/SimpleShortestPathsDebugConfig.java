package stanford.infolab.debugger.instrumenter.examples;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

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
    return vertexId.get() == 1L;
  }
  
  public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> clazz = Class.forName(
      "stanford.infolab.debugger.instrumenter.examples.SimpleShortestPathsDebugConfig");
    DebugConfig debugConfig = (DebugConfig) clazz.newInstance();
    ParameterizedType parameterizedType = (ParameterizedType) clazz.getGenericSuperclass();
    System.out.println(((Class<LongWritable>) parameterizedType.getActualTypeArguments()[0]).getCanonicalName());
    
//    for (Type type : parameterizedType.getActualTypeArguments()) {
//      System.out.println("type: " + type);
//      System.out.println(((Class<?>) type.getClass()).getCanonicalName());
//    }
  }
  
  public void run() {
    
  }
}
