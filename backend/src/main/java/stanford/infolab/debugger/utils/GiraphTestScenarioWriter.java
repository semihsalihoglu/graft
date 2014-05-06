package stanford.infolab.debugger.utils;

import java.io.IOException;

import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.NeighborWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.OutgoingMessageWrapper;

/**
 * Temporary utility class to generate test protocol buffers that store scenarios.
 * 
 * @author semihsalihoglu
 */
public class GiraphTestScenarioWriter {

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    String outputFileName = args[0];
    System.out.println("outputFileName: " + outputFileName);

    // Write a scenario that contains:
    // 1) vertexID: 89
    // 2) vertexValue: -5
    // 3) nbrs: 0, 1, ..., 7
    // 4) in-msgs: 0.0, 10.0, 20.0, ..., 50.0
    // 5) out-msgs: nbr O -> 0.0, nbr 1 -> 1.0, nbr 2 -> 2.0
    GiraphScenarioWrapper<LongWritable, DoubleWritable, FloatWritable, DoubleWritable,
      DoubleWritable> giraphScenarioWrapper = new GiraphScenarioWrapper<>(
        SimpleShortestPathsComputation.class, LongWritable.class, DoubleWritable.class,
        FloatWritable.class, DoubleWritable.class, DoubleWritable.class);
    GiraphScenarioWrapper<LongWritable, DoubleWritable, FloatWritable, DoubleWritable,
      DoubleWritable>.ContextWrapper contextWrapper = giraphScenarioWrapper.new ContextWrapper();
    contextWrapper.setVertexIdWrapper(new LongWritable(89));
    contextWrapper.setVertexValueBeforeWrapper(new DoubleWritable(-5));
    for (int i = 0; i <= 7; ++i) {
      contextWrapper.addNeighborWrapper(new LongWritable(i), new FloatWritable(1));
    }
    for (int i = 0; i <= 5; ++i) {
      contextWrapper.addIncomingMessageWrapper(new DoubleWritable(i * 10.0));
    }
    for (int i = 0; i <= 2; ++i) {
      contextWrapper.addOutgoingMessageWrapper(new LongWritable(i), new DoubleWritable(i * -10.0));
    }
    giraphScenarioWrapper.setContextWrapper(contextWrapper);

    new GiraphScenearioSaverLoader<LongWritable, DoubleWritable, FloatWritable, DoubleWritable,
      DoubleWritable>().save(outputFileName, giraphScenarioWrapper);

    // Deserialize the written pb, and verify that the deserialized scenario contains the same
    // contents.
//    GiraphScenarioWrapper<LongWritable, DoubleWritable, FloatWritable, DoubleWritable,
//      DoubleWritable> deserializedGiraphScenarioWrapper = new GiraphScenearioSaverLoader<
//        LongWritable, DoubleWritable, FloatWritable, DoubleWritable, DoubleWritable>().load(
//          outputFileName);
//    System.out.println("deserizalied giraph scenario:");
//    System.out.println(deserializedGiraphScenarioWrapper);
//    GiraphScenarioWrapper<LongWritable, DoubleWritable, FloatWritable, DoubleWritable,
//      DoubleWritable>.ContextWrapper deserializedContext = giraphScenarioWrapper.getContextWrapper();
//    System.out.println("deserializedScenario.vertexID: " + deserializedContext.getVertexIdWrapper());
//    System.out.println("deserializedScenario.vertexValue: "
//      + deserializedContext.getVertexValueWrapper());
//    for (NeighborWrapper neighborWrapper : deserializedContext.getNeighborWrappers()) {
//      System.out.println("deserialized neighborId: " + neighborWrapper.getNbrId() + " value: "
//        + neighborWrapper.getNbrId());
//    }
//    for (DoubleWritable neighborId : deserializedContext.getIncomingMessageWrappers()) {
//      System.out.println("incoming message: " + neighborId.get());
//    }
//    for (OutgoingMessageWrapper outgoingMessage : deserializedContext.getOutgoingMessageWrappers()) {
//      System.out.println("outgoing message. nbrId: " + outgoingMessage.destinationId + " msg: "
//        + outgoingMessage.message);
//    }
  }
}
