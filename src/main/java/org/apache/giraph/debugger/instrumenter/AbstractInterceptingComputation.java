package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.debugger.DebugConfig;
import org.apache.giraph.debugger.utils.ExceptionWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper;
import org.apache.giraph.debugger.utils.MsgIntegrityViolationWrapper;
import org.apache.giraph.debugger.utils.VertexValueIntegrityViolationWrapper;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Class that intercepts call to the AbstractComputation's exposed methods for
 * GiraphDebugger.
 * 
 * @author semihsalihoglu
 * @author netj
 * 
 * @param <I>
 *          Vertex id
 * @param <V>
 *          Vertex data
 * @param <E>
 *          Edge data
 * @param <M1>
 *          Incoming message type
 * @param <M2>
 *          Outgoing message type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class AbstractInterceptingComputation<I extends WritableComparable, V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
  extends AbstractComputation<I, V, E, M1, M2> {

  private static final String JAR_SIGNATURE_KEY = "giraph.debugger.jarSignature";

  public static final String CONFIG_CLASS_KEY = "giraph.debugger.configClass";

  protected static final Logger LOG = Logger.getLogger(AbstractInterceptingComputation.class);

  public static final StrConfOption DEBUG_CONFIG_CLASS = new StrConfOption(CONFIG_CLASS_KEY,
    DebugConfig.class.getName(),
    "The name of the Debug Config class for the computation (e.g. "
      + "org.apache.giraph.debugger.examples.SimpleShortestPathsDebugConfig).");

  private static DebugConfig debugConfig;
  private static Type vertexIdClazz;
  private static Type vertexValueClazz;
  private static Type edgeValueClazz;
  private static Type incomingMessageClazz;
  private static Type outgoingMessageClazz;
  private boolean shouldDebugVertex = false;
  private GiraphVertexScenarioWrapper<I, V, E, M1, M2> giraphVertexScenarioWrapper;
  private MsgIntegrityViolationWrapper<I, M2> msgIntegrityViolationWrapper;
  private VertexValueIntegrityViolationWrapper<I, V> vertexValueIntegrityViolationWrapper;
  // We store the vertexId here in case some functions need it.
  private I vertexId;
  // Contains previous aggregators that are available in the beginning of the
  // superstep.In Giraph, these aggregators are immutable.
  // NOTE: We currently only capture aggregators that are read by at least one
  // vertex. If we want to capture all aggregators we need to change Giraph code to be
  // get access to them.
  private CommonVertexMasterInterceptionUtil commonVertexMasterInterceptionUtil;
  private static int NUM_VIOLATIONS_TO_LOG = 10;
  private static int NUM_VERTICES_TO_LOG = 10;
  private static int numVerticesLogged = -1;

  final protected void interceptInitializeEnd() {
    commonVertexMasterInterceptionUtil = new CommonVertexMasterInterceptionUtil(
      getContext().getJobID().toString());

    String debugConfigFileName = DEBUG_CONFIG_CLASS.get(getConf());
    LOG.info("debugConfigFileName: " + debugConfigFileName);
    Class<?> clazz;
    try {
      clazz = Class.forName(debugConfigFileName);
      debugConfig = (DebugConfig<I, V, E, M1, M2>) clazz.newInstance();
      debugConfig.readConfig(getConf());
      LOG.info("Successfully created a DebugConfig file from: " + debugConfigFileName);
      vertexIdClazz = getConf().getVertexIdClass();
      vertexValueClazz = getConf().getVertexValueClass();
      edgeValueClazz = getConf().getEdgeValueClass();
      incomingMessageClazz = getConf().getIncomingMessageValueClass();
      outgoingMessageClazz =getConf().getOutgoingMessageValueClass();
    } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
      LOG.error("Could not create a new DebugConfig instance of " + debugConfigFileName);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // record jar signature if necessary
    String jarSignature = getConf().get(JAR_SIGNATURE_KEY);
    if (jarSignature != null) {
      FileSystem fs = CommonVertexMasterInterceptionUtil.getFileSystem();
      Path jarSignaturePath = new Path(commonVertexMasterInterceptionUtil.getTraceDirectory() + "/"
        + "jar.signature");
      try {
        if (!fs.exists(jarSignaturePath)) {
          OutputStream f = fs.create(jarSignaturePath, true).getWrappedStream();
          IOUtils.write(jarSignature, f);
        }
      } catch (IOException e) {
        // When multiple workers try to write the jar.signature, some of them may cause
        // AlreadyBeingCreatedException to be thrown, which we ignore.
        e.printStackTrace();
      }
    }
  }

  /**
   * Called immediately when the compute() method is entered. Initializes data
   * that will be required for debugging throughout the rest of the compute
   * function.
   * 
   * @return whether the user has specified catching exceptions.
   */
  final protected boolean interceptComputeBegin(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    // We first figure out whether we should be debugging this vertex in
    // this iteration.
    // Other calls will use the value of shouldDebugVertex later on.
    LOG.debug("compute " + vertex + " " + messages);
    vertexId = vertex.getId();
    shouldDebugVertex = debugConfig.shouldDebugSuperstep(getSuperstep())
      && debugConfig.shouldDebugVertex(vertex) && (numVerticesLogged <= NUM_VERTICES_TO_LOG);
    if (shouldDebugVertex) {
      initGiraphVertexScenario();
      debugVertexBeforeComputation(vertex, messages);
    }
    return debugConfig.shouldCatchExceptions();
  }

  final protected void interceptComputeException(Vertex<I, V, E> vertex, Iterable<M1> messages,
    Exception e) throws IOException {
    LOG.info("LOG.info: Caught an exception. message: " + e.getMessage()
      + ". Saving a trace in HDFS.");
    // We initialize the giraph scenario from scratch.
    initGiraphVertexScenario();
    debugVertexBeforeComputation(vertex, messages);
    ExceptionWrapper exceptionWrapper = new ExceptionWrapper(e.getMessage(),
      ExceptionUtils.getStackTrace(e));
    giraphVertexScenarioWrapper.setExceptionWrapper(exceptionWrapper);
    saveGiraphVertexScenarioWrapper(vertex, true /* is exception vertex */);
  }

  final protected void interceptComputeEnd(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    if (shouldDebugVertex) {
      giraphVertexScenarioWrapper.getContextWrapper().setVertexValueAfterWrapper(vertex.getValue());
      saveGiraphVertexScenarioWrapper(vertex, false /* not exception vertex */);
      numVerticesLogged++;
    }
    if (debugConfig.shouldCheckVertexValueIntegrity()
      && !debugConfig.isVertexValueCorrect(vertexId, vertex.getValue())
      && vertexValueIntegrityViolationWrapper.numVerteIdValuePairWrappers() <= NUM_VIOLATIONS_TO_LOG) {
      vertexValueIntegrityViolationWrapper.addVertexIdPairWrapper(vertexId, vertex.getValue());
    }
  }

  private void debugVertexBeforeComputation(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException {
    giraphVertexScenarioWrapper.getContextWrapper().setVertexIdWrapper(vertex.getId());
    giraphVertexScenarioWrapper.getContextWrapper().setVertexValueBeforeWrapper(vertex.getValue());
    Iterable<Edge<I, E>> returnVal = vertex.getEdges();
    for (Edge<I, E> edge : returnVal) {
      if (edge.getTargetVertexId() == null) {
        LOG.debug("the targetVertexId is null!!!");
      } else if (edge.getValue() == null) {
        LOG.debug("edge value is null!!! targetVertexId: " + edge.getTargetVertexId());
      }
      giraphVertexScenarioWrapper.getContextWrapper().addNeighborWrapper(edge.getTargetVertexId(),
        edge.getValue());
    }
    for (M1 message : messages) {
      giraphVertexScenarioWrapper.getContextWrapper().addIncomingMessageWrapper(message);
    }
  }

  private void initGiraphVertexScenario() {
    giraphVertexScenarioWrapper = new GiraphVertexScenarioWrapper(getActualTestedClass(),
      (Class<I>) vertexIdClazz, (Class<V>) vertexValueClazz, (Class<E>) edgeValueClazz,
      (Class<M1>) incomingMessageClazz, (Class<M2>) outgoingMessageClazz);
    VertexContextWrapper contextWrapper = giraphVertexScenarioWrapper.new VertexContextWrapper();
    giraphVertexScenarioWrapper.setContextWrapper(contextWrapper);
    commonVertexMasterInterceptionUtil.initCommonVertexMasterContextWrapper(getConf(),
      getSuperstep(), getTotalNumVertices(), getTotalNumEdges());
    contextWrapper.setCommonVertexMasterContextWrapper(
      commonVertexMasterInterceptionUtil.getCommonVertexMasterContextWrapper());
  }

  private void saveGiraphVertexScenarioWrapper(Vertex<I, V, E> vertex, boolean isExceptionVertex)
    throws IOException {
    commonVertexMasterInterceptionUtil.saveVertexScenarioWrapper(giraphVertexScenarioWrapper,
      vertexId.toString(), isExceptionVertex);
  }

  /**
   * First intercepts the sent message if necessary and calls and then calls
   * AbstractComputation's sendMessage method.
   * 
   * 
   * @param id
   *          Vertex id to send the message to
   * @param message
   *          Message data to send
   */
  @Override
  public void sendMessage(I id, M2 message) {
    interceptMessageAndCheckIntegrityIfNecessary(id, message);
    super.sendMessage(id, message);
  }

  private void interceptMessageAndCheckIntegrityIfNecessary(I id, M2 message) {
    if (shouldDebugVertex) {
      giraphVertexScenarioWrapper.getContextWrapper().addOutgoingMessageWrapper(id, message);
    }
    if (debugConfig.shouldCheckMessageIntegrity()
      && !debugConfig.isMessageCorrect(id, vertexId, message)
      && msgIntegrityViolationWrapper.numMsgWrappers() <= NUM_VIOLATIONS_TO_LOG) {
      msgIntegrityViolationWrapper.addMsgWrapper(vertexId, id, message);
    }
  }

  /**
   * First intercepts the sent messages to all edges if necessary and calls and
   * then calls AbstractComputation's sendMessageToAllEdges method.
   * 
   * @param vertex
   *          Vertex whose edges to send the message to.
   * @param message
   *          Message sent to all edges.
   */
  @Override
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message) {
    for (Edge<I, E> edge : vertex.getEdges()) {
      interceptMessageAndCheckIntegrityIfNecessary(edge.getTargetVertexId(), message);
    }
    super.sendMessageToAllEdges(vertex, message);
  }

  final protected void interceptPreSuperstepBegin() {
    numVerticesLogged = 0;
    if (debugConfig.shouldCheckMessageIntegrity()) {
      LOG.info("creating a msgIntegrityViolationWrapper. superstepNo: " + getSuperstep());
      msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper<>((Class<I>) vertexIdClazz,
        (Class<M2>) outgoingMessageClazz);
      msgIntegrityViolationWrapper.setSuperstepNo(getSuperstep());
    }
    if (debugConfig.shouldCheckVertexValueIntegrity()) {
      LOG.info("creating a vertexValueViolationWrapper. superstepNo: " + getSuperstep());
      vertexValueIntegrityViolationWrapper = new VertexValueIntegrityViolationWrapper(
        (Class<I>) vertexIdClazz, (Class<V>) vertexValueClazz);
      vertexValueIntegrityViolationWrapper.setSuperstepNo(getSuperstep());
    }
  }

  final protected void interceptPostSuperstepEnd() {
    if (debugConfig.shouldCheckMessageIntegrity()
      && msgIntegrityViolationWrapper.numMsgWrappers() > 0) {
      try {
        // TODO(semih): Test at large scale with multiple workers.
        msgIntegrityViolationWrapper.saveToHDFS(commonVertexMasterInterceptionUtil.getFileSystem(),
          getViolationTraceFile(true /* is message violation */));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (debugConfig.shouldCheckVertexValueIntegrity()
      && vertexValueIntegrityViolationWrapper.numVerteIdValuePairWrappers() > 0) {
      try {
        vertexValueIntegrityViolationWrapper.saveToHDFS(
          commonVertexMasterInterceptionUtil.getFileSystem(),
          getViolationTraceFile(false /* is vertex violation */));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String getViolationTraceFile(boolean isMessageViolation) {
    return commonVertexMasterInterceptionUtil.getTraceDirectory() + "/task_"
      + getContext().getTaskAttemptID().getTaskID().getId() + (isMessageViolation? "_msg" : "_vv")
      + "_intgrty_stp_" + getSuperstep() + ".tr";
  }

  public abstract Class<? extends Computation<I, V, E, ? extends Writable, ? extends Writable>> getActualTestedClass();

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    A retVal = super.<A> getAggregatedValue(name);
    commonVertexMasterInterceptionUtil.addAggregatedValueIfNotExists(name, retVal);
    return retVal;
  }
}