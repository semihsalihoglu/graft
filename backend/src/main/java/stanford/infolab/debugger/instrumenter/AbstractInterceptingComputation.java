package stanford.infolab.debugger.instrumenter;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper;

/**
 * Class that intercepts call to the AbstractComputation's exposed methods for GiraphDebugger.
 * 
 * @author semihsalihoglu
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public abstract class AbstractInterceptingComputation<I extends WritableComparable,
  V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
  extends AbstractComputation<I, V, E, M1, M2> {

  public static final StrConfOption DEBUG_CONFIG_CLASS =
    new StrConfOption("dbgcfg", "", "The name of the Debug Config class for the computation (e.g. "
      + "stanford.infolab.debugger.instrumenter.examples.SimpleShortestPathsDebugConfig).");

  @SuppressWarnings("rawtypes")
  private static DebugConfig debugConfig;
  private boolean shouldDebugVertex = false;
  private GiraphScenarioWrapper<I, V, E, M1, M2> giraphScenarioWrapper;
  private GiraphScenarioWrapper<I, V, E, M1, M2>.ContextWrapper contextWrapper;
  @SuppressWarnings("unchecked")
  @Override
  public void initialize(
      GraphState graphState,
      WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
      GraphTaskManager<I, V, E> graphTaskManager,
      WorkerAggregatorUsage workerAggregatorUsage,
      WorkerContext workerContext) {
    // We first call super.initialize so that the getConf() call below returns a non-null value.
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerAggregatorUsage, workerContext);
 
    // If we haven't initilized debugConfig, we first instantiate it, and then we instantiate
    // giraphScenarioWrapper.
    if (debugConfig == null) {
      String debugConfigFileName = DEBUG_CONFIG_CLASS.get(getConf());
      System.out.println("debugConfigFileName: " + debugConfigFileName);
      Class<?> clazz;
      try {
        clazz = Class.forName(debugConfigFileName);
        debugConfig = (DebugConfig<I, V, E, M1, M2>) clazz.newInstance();
        System.out.println("Successfully created a DebugConfig file from: " + debugConfigFileName);
      } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
        System.err.println("Could not create a new DebugConfig instance of " + debugConfigFileName);
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      
      System.out.println(this.getClass().getCanonicalName());
      ParameterizedType parameterizedType = (ParameterizedType) clazz.getGenericSuperclass();
      System.out.println(((Class<I>) parameterizedType.getActualTypeArguments()[0]).getCanonicalName());
      System.out.println(((Class<V>) parameterizedType.getActualTypeArguments()[1]).getCanonicalName());
      System.out.println(((Class<E>) parameterizedType.getActualTypeArguments()[2]).getCanonicalName());
      System.out.println(((Class<M1>) parameterizedType.getActualTypeArguments()[3]).getCanonicalName());
      System.out.println(((Class<M2>) parameterizedType.getActualTypeArguments()[4]).getCanonicalName());
      
      giraphScenarioWrapper = new GiraphScenarioWrapper(this.getClass(), 
        (Class<I>) parameterizedType.getActualTypeArguments()[0], 
        (Class<V>) parameterizedType.getActualTypeArguments()[1],
        (Class<E>) parameterizedType.getActualTypeArguments()[2],
        (Class<M1>) parameterizedType.getActualTypeArguments()[3],
        (Class<M2>) parameterizedType.getActualTypeArguments()[4]);
    }
  }

  public void compute(Vertex<I, V, E> vertex, Iterable<M1> messages) throws IOException {
    // We first figure out whether we should be debugging this vertex in this iteration.
    // Other calls will use the value of shouldDebugVertex later on.
    shouldDebugVertex = debugConfig.shouldDebugSuperstep(getSuperstep()) &&
        debugConfig.shouldDebugVertex(vertex.getId());
    debugVertexBeforeComputationIfConfigured(vertex, messages);
    computeFurther(vertex, messages);
//    debugVertexValueIfConfigured(vertex, false /* is after */, contextWrapper);
  }

  public abstract void computeFurther(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException;
  
  /**
   * First intercepts the sent message if necessary and calls and then
   * calls AbstractComputation's sendMessage method. 

   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  @Override
  public void sendMessage(I id, M2 message) {
    if (shouldDebugVertex) {
      System.out.println("vertex is sending a message to: " + id + " msg: " + message);
    }
    super.sendMessage(id, message);
  }

  /**
   * First intercepts the sent messages to all edges if necessary and calls and then
   * calls AbstractComputation's sendMessageToAllEdges method. 
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  @Override
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message) {
    if (shouldDebugVertex) {
      System.out.print("vertex is sending a message to all neighbors. msg: " + message);
      for (Edge<I, E> edge : vertex.getEdges()) {
        System.out.print("\tnbrId: " + edge.getTargetVertexId());
      }
      System.out.println();
    }
    super.sendMessageToAllEdges(vertex, message);
  }

  private void debugVertexBeforeComputationIfConfigured(Vertex<I, V, E> vertex,
    Iterable<M1> messages) {
    contextWrapper = giraphScenarioWrapper.new ContextWrapper();
    debugVertexIfConfigured(vertex, true /* is before */, contextWrapper);
    if (shouldDebugVertex) {
      for (M1 message : messages) {
        System.out.print("\tin-msg: " + message);
      }
      System.out.println();
    }
    giraphScenarioWrapper.setContextWrapper(contextWrapper);
  }
  private void debugVertexIfConfigured(Vertex<I, V, E> vertex, boolean isBefore,
    GiraphScenarioWrapper<I, V, E, M1, M2>.ContextWrapper contextWrapper) {
    if (shouldDebugVertex) {
      System.out.println("-----DEBUGGING VERTEX " + (isBefore ? "BEFORE COMPUTATION: "
        : "AFTER COMPUTATION: ") + vertex.getId() + "-----");
      System.out.print("superstep: " + getSuperstep());
      System.out.print("vertex.ID: " + vertex.getId());
      contextWrapper.setVertexIdWrapper(vertex.getId());
      System.out.println("\tvertex.Value: " + vertex.getValue());
      System.out.print("\tvertex.Value: " + vertex.getValue());
      Iterable<Edge<I, E>> returnVal = vertex.getEdges();
      System.out.print("vertex.getEdges:");
      for (Edge<I, E> edge : returnVal) {
        System.out.print("\tedge: <" + edge.getTargetVertexId() + ", " + edge.getValue()
          + ">");
      }
      System.out.println("\n-----FINISHED DEBUGGING VERTEX " + (isBefore ? "BEFORE COMPUTATION: "
        : "AFTER COMPUTATION: ") + vertex.getId() + "-----");
    }
  }
}