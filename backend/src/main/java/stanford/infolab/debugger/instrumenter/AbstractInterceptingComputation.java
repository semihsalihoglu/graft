package stanford.infolab.debugger.instrumenter;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ExceptionWrapper;
import stanford.infolab.debugger.utils.AggregatedValueWrapper;
import stanford.infolab.debugger.utils.MsgIntegrityViolationWrapper;
import stanford.infolab.debugger.utils.VertexValueIntegrityViolationWrapper;

/**
 * Class that intercepts call to the AbstractComputation's exposed methods for GiraphDebugger.
 * 
 * @author semihsalihoglu
 * @author netj
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractInterceptingComputation<I extends WritableComparable,
  V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
  extends AbstractComputation<I, V, E, M1, M2> {

  protected static final Logger LOG =
    Logger.getLogger(AbstractInterceptingComputation.class);

  public static final StrConfOption DEBUG_CONFIG_CLASS =
    new StrConfOption("dbgcfg", "", "The name of the Debug Config class for the computation (e.g. "
      + "stanford.infolab.debugger.examples.SimpleShortestPathsDebugConfig).");

  private static DebugConfig debugConfig;
  private static Type vertexIdClazz;
  private static Type vertexValueClazz;
  private static Type edgeValueClazz;
  private static Type incomingMessageClazz;
  private static Type outgoingMessageClazz;
  private boolean shouldDebugVertex = false;
  private GiraphScenarioWrapper<I, V, E, M1, M2> giraphScenarioWrapper;
  private MsgIntegrityViolationWrapper<I, M2> msgIntegrityViolationWrapper;
  private VertexValueIntegrityViolationWrapper<I, V> vertexValueIntegrityViolationWrapper;
  // We store the vertexId here in case some functions need it.
  private I vertexId;
  private static FileSystem fileSystem = null;
  // Contains previous aggregators that are available in the beginning of the superstep.
  // In Giraph, these aggregators are immutable.
  // NOTE: We currently only capture aggregators that are read by at least one vertex.
  // If we want to capture all aggregators we need to change Giraph code to be get access to them.
  private ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers;
  private static int NUM_VIOLATIONS_TO_LOG = 10;

  final protected void interceptInitializeEnd() {
    String debugConfigFileName = DEBUG_CONFIG_CLASS.get(getConf());
    System.out.println("debugConfigFileName: " + debugConfigFileName);
    Class<?> clazz;
    try {
      clazz = Class.forName(debugConfigFileName);
      debugConfig = (DebugConfig<I, V, E, M1, M2>) clazz.newInstance();
      System.out.println("Successfully created a DebugConfig file from: " + debugConfigFileName);
      ParameterizedType parameterizedType = (ParameterizedType) debugConfig.getClass()
        .getGenericSuperclass();
      vertexIdClazz = parameterizedType.getActualTypeArguments()[0];
      vertexValueClazz = parameterizedType.getActualTypeArguments()[1];
      edgeValueClazz = parameterizedType.getActualTypeArguments()[2];
      incomingMessageClazz = parameterizedType.getActualTypeArguments()[3];
      outgoingMessageClazz = parameterizedType.getActualTypeArguments()[4];
    } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
      System.err.println("Could not create a new DebugConfig instance of " + debugConfigFileName);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  // Called immediately the compute() method is entered. Initializes data that will be required
  // for debugging throughout the rest of the compute function.
  final protected boolean interceptComputeBegin(Vertex<I, V, E> vertex, Iterable<M1> messages) throws IOException {
    // We first figure out whether we should be debugging this vertex in
    // this iteration.
    // Other calls will use the value of shouldDebugVertex later on.
    LOG.info("compute " + vertex + " " + messages);
    vertexId = vertex.getId();
    shouldDebugVertex = debugConfig.shouldDebugSuperstep(getSuperstep()) &&
      debugConfig.shouldDebugVertex(vertex.getId());
    if (shouldDebugVertex) {
      initGiraphScenario();
      debugVertexBeforeComputation(vertex, messages);
    }
    return debugConfig.shouldCatchExceptions();
  }
  
  final protected void interceptComputeException(Vertex<I, V, E> vertex, Iterable<M1> messages, Exception e)
		  throws IOException {
    System.out
		.println("System.out.println: Caught an exception. message: "
				+ e.getMessage()
				+ ". Saving a trace in HDFS.");
      LOG.info("LOG.info: Caught an exception. message: "
		+ e.getMessage() + ". Saving a trace in HDFS.");
	  // We initialize the giraph scenario from scratch.
	  initGiraphScenario();
	  debugVertexBeforeComputation(vertex, messages);
	  giraphScenarioWrapper.setExceptionWrapper(new ExceptionWrapper(e.getMessage(),
			  ExceptionUtils.getStackTrace(e)));
	  saveGiraphWrapper(vertex, true /* is exception vertex */);
  }
  
  final protected void interceptComputeEnd(Vertex<I, V, E> vertex, Iterable<M1> messages) throws IOException {
	  if (shouldDebugVertex) {
		  giraphScenarioWrapper.getContextWrapper().setVertexValueAfterWrapper(vertex.getValue());
		  saveGiraphWrapper(vertex, false /* not exception vertex */);
	  }
	  if (debugConfig.shouldCheckVertexValueIntegrity() &&
			  !debugConfig.isVertexValueCorrect(vertexId, vertex.getValue()) &&
			  vertexValueIntegrityViolationWrapper.numVerteIdValuePairWrappers() <= NUM_VIOLATIONS_TO_LOG) {
		  System.out.println("adding a vertex id pair to vertexValueIntegrityViolationWrapper");
		  vertexValueIntegrityViolationWrapper.addVertexIdPairWrapper(vertexId, vertex.getValue());
	  }
  }

  private void debugVertexBeforeComputation(Vertex<I, V, E> vertex, Iterable<M1> messages) throws IOException {
    giraphScenarioWrapper.getContextWrapper().setSuperstepNoWrapper(getSuperstep());
    giraphScenarioWrapper.getContextWrapper().setVertexIdWrapper(vertex.getId());
    giraphScenarioWrapper.getContextWrapper().setVertexValueBeforeWrapper(vertex.getValue());
    Iterable<Edge<I, E>> returnVal = vertex.getEdges();
    for (Edge<I, E> edge : returnVal) {
      if (edge.getTargetVertexId() == null) {
        System.out.println("the targetVertexId is null!!!");
      } else if (edge.getValue() == null) {
        System.out.println("edge value is null!!! targetVertexId: " + edge.getTargetVertexId());
      }
      giraphScenarioWrapper.getContextWrapper().addNeighborWrapper(edge.getTargetVertexId(),
        edge.getValue());
    }
    for (M1 message : messages) {
      giraphScenarioWrapper.getContextWrapper().addIncomingMessageWrapper(message);
    }
  }

private void initGiraphScenario() {
    giraphScenarioWrapper = new GiraphScenarioWrapper(getActualTestedClass(),
      (Class<I>) vertexIdClazz, (Class<V>) vertexValueClazz, (Class<E>) edgeValueClazz,
      (Class<M1>) incomingMessageClazz, (Class<M2>) outgoingMessageClazz);
    giraphScenarioWrapper.setConfig(getConf());
    giraphScenarioWrapper.setContextWrapper(giraphScenarioWrapper.new ContextWrapper());
    giraphScenarioWrapper.getContextWrapper().setPreviousAggregatedValues(
      previousAggregatedValueWrappers);
    giraphScenarioWrapper.getContextWrapper().setTotalNumVerticesWrapper(getTotalNumVertices());
    giraphScenarioWrapper.getContextWrapper().setTotalNumEdgesWrapper(getTotalNumEdges());
  }

  private void saveGiraphWrapper(Vertex<I, V, E> vertex, boolean isExceptionVertex)
    throws IOException {
    String suffix = isExceptionVertex ? "err" : "reg";
    String fileName = "/giraph-debug-traces/" + getContext().getJobID()
      + "/" + suffix +"_stp_" + getSuperstep() + "_vid_" + vertex.getId() + ".tr";
    giraphScenarioWrapper.saveToHDFS(fileSystem, fileName);
  }
  
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
      giraphScenarioWrapper.getContextWrapper().addOutgoingMessageWrapper(id, message);
    }
    if (debugConfig.shouldCheckMessageIntegrity() &&
      !debugConfig.isMessageCorrect(id, vertexId, message) &&
      msgIntegrityViolationWrapper.numMsgWrappers() <= NUM_VIOLATIONS_TO_LOG) {
      msgIntegrityViolationWrapper.addMsgWrapper(vertexId, id, message);
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
      // TODO(semih): Intercept
    }
    super.sendMessageToAllEdges(vertex, message);
  }
  
  final protected void interceptPreSuperstepBegin() {
	if (fileSystem == null) {
      try {
        fileSystem = FileSystem.get(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    previousAggregatedValueWrappers = new ArrayList<AggregatedValueWrapper>();
    if (debugConfig.shouldCheckMessageIntegrity()) {
      System.out.println("creating a msgIntegrityViolationWrapper. superstepNo: " + getSuperstep());
      msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper<>((Class<I>) vertexIdClazz,
        (Class<M2>) outgoingMessageClazz);
      msgIntegrityViolationWrapper.setSuperstepNo(getSuperstep());
    }
    if (debugConfig.shouldCheckVertexValueIntegrity()) {
      System.out.println("creating a vertexValueViolationWrapper. superstepNo: " + getSuperstep());
      vertexValueIntegrityViolationWrapper = new VertexValueIntegrityViolationWrapper(
        (Class<I>) vertexIdClazz, (Class<V>) vertexValueClazz);
      vertexValueIntegrityViolationWrapper.setSuperstepNo(getSuperstep());
    }
  }


  final protected void interceptPostSuperstepEnd() {
	if (debugConfig.shouldCheckMessageIntegrity()
      && msgIntegrityViolationWrapper.numMsgWrappers() > 0) {
      try {
        // TODO(semih): Learn how to read the id of a worker so we output 
        // one trace for each worker. Right now only one trace is output.
        String fileName = "/giraph-debug-traces/" + getContext().getJobID()
          + "/msg_intgrty_stp_" + getSuperstep() + ".tr";
        msgIntegrityViolationWrapper.saveToHDFS(fileSystem, fileName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (debugConfig.shouldCheckVertexValueIntegrity()
      && vertexValueIntegrityViolationWrapper.numVerteIdValuePairWrappers() > 0) {
      try {
        String fileName = "/giraph-debug-traces/" + getContext().getJobID()
          + "/vv_intgrty_stp_" + getSuperstep() + ".tr";
        vertexValueIntegrityViolationWrapper.saveToHDFS(fileSystem, fileName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
}
  
  public abstract Class<? extends Computation<I,V,E,? extends Writable,? extends Writable>> getActualTestedClass();

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    A retVal = super.<A>getAggregatedValue(name);
    if (getPreviousAggregatedValueWrapper(name) == null && retVal != null) {
      previousAggregatedValueWrappers.add(new AggregatedValueWrapper(name, retVal));
    }
    return retVal;
  }

  private AggregatedValueWrapper getPreviousAggregatedValueWrapper(String key) {
    for (AggregatedValueWrapper previousAggregatedValueWrapper
      : previousAggregatedValueWrappers) {
      if (key.equals(previousAggregatedValueWrapper.getKey())) {
        return previousAggregatedValueWrapper;
      }
    }
    return null;
  }
}