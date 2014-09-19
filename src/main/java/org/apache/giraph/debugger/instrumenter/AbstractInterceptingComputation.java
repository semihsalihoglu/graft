/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.debugger.DebugConfig;
import org.apache.giraph.debugger.utils.DebuggerUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.ExceptionWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper;
import org.apache.giraph.debugger.utils.MsgIntegrityViolationWrapper;
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

  protected static final Logger LOG = Logger
    .getLogger(AbstractInterceptingComputation.class);

  public static final StrConfOption DEBUG_CONFIG_CLASS = new StrConfOption(
    CONFIG_CLASS_KEY, DebugConfig.class.getName(),
    "The name of the Debug Config class for the computation (e.g. "
      + "org.apache.giraph.debugger.examples.SimpleShortestPathsDebugConfig).");

  private static DebugConfig debugConfig;
  private static Type vertexIdClazz;
  private static Type vertexValueClazz;
  private static Type edgeValueClazz;
  private static Type incomingMessageClazz;
  private static Type outgoingMessageClazz;
  private MsgIntegrityViolationWrapper<I, M2> msgIntegrityViolationWrapper;

  // Stores the value of a vertex before the compute method is called. If a
  // vertex throws an
  // exception, or violates a vertex or message value constraint, then we use
  // this value as the
  // previous vertex value when we save a vertexScenario trace for it.
  private V previousVertexValue;
  // If a vertex has violated a message value constraint when it was sending a
  // message
  // we set this to true so that at the inside interceptComputeEnd() method we
  // make
  // sure we save a vertexScenario trace for it.
  private boolean hasViolatedMsgValueConstraint;
  // We store the vertexId here in case some functions need it.
  private I vertexId;
  // Whether or not this vertex was configured to be debugged. If so we will
  // intercept
  // its outgoing messages.
  private boolean shouldDebugVertex;
  // For vertices that are configured to be debugged, we construct a
  // GiraphVertexScenarioWrapper
  // in the beginning and use it to intercept outgoing messages
  private GiraphVertexScenarioWrapper<I, V, E, M1, M2> giraphVertexScenarioWrapperForRegularTraces;
  // Contains previous aggregators that are available in the beginning of the
  // superstep.In Giraph, these aggregators are immutable.
  // NOTE: We currently only capture aggregators that are read by at least one
  // vertex. If we want to capture all aggregators we need to change Giraph code
  // to be
  // get access to them.
  private CommonVertexMasterInterceptionUtil commonVertexMasterInterceptionUtil;

  private static int NUM_VIOLATIONS_TO_LOG = 10;
  private static int NUM_VERTICES_TO_LOG = 10;
  private static int numVerticesLogged = -1;
  private static int numVertexViolationsLogged = -1;
  private static int numMessageViolationsLogged = -1;

  final protected void interceptInitializeEnd() {
    initializeAbstractInterceptingComputation();
  }

  private void initializeAbstractInterceptingComputation() {
    commonVertexMasterInterceptionUtil = new CommonVertexMasterInterceptionUtil(
      getContext().getJobID().toString());

    String debugConfigClassName = DEBUG_CONFIG_CLASS.get(getConf());
    LOG.info("debugConfigClass: " + debugConfigClassName);
    Class<?> clazz;
    try {
      clazz = Class.forName(debugConfigClassName);
      debugConfig = (DebugConfig<I, V, E, M1, M2>) clazz.newInstance();
      debugConfig.readConfig(getConf());
      LOG.debug("Successfully created a DebugConfig file from: " +
        debugConfigClassName);
      vertexIdClazz = getConf().getVertexIdClass();
      vertexValueClazz = getConf().getVertexValueClass();
      edgeValueClazz = getConf().getEdgeValueClass();
      incomingMessageClazz = getConf().getIncomingMessageValueClass();
      outgoingMessageClazz = getConf().getOutgoingMessageValueClass();
    } catch (InstantiationException | ClassNotFoundException |
      IllegalAccessException e) {
      LOG.error("Could not create a new DebugConfig instance of " +
        debugConfigClassName);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    // record jar signature if necessary
    String jarSignature = getConf().get(JAR_SIGNATURE_KEY);
    if (jarSignature != null) {
      FileSystem fs = commonVertexMasterInterceptionUtil.getFileSystem();
      Path jarSignaturePath = new Path(
        DebuggerUtils.getTraceFileRoot(commonVertexMasterInterceptionUtil
          .getJobId()) + "/" + "jar.signature");
      try {
        if (!fs.exists(jarSignaturePath)) {
          OutputStream f = fs.create(jarSignaturePath, true).getWrappedStream();
          IOUtils.write(jarSignature, f);
          f.close();
        }
      } catch (IOException e) {
        // When multiple workers try to write the jar.signature, some of them
        // may cause
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
  protected final boolean interceptComputeBegin(Vertex<I, V, E> vertex,
    Iterable<M1> messages) throws IOException {
    LOG.debug("compute " + vertex + " " + messages);
    if (debugConfig == null) {
      // TODO: Sometimes Giraph doesn't call initialize() and directly calls
      // compute(). Here we
      // guard against things not being initiliazed, which was causing null
      // pointer exceptions.
      // Find out when/why this happens.
      LOG.warn("interceptComputeBegin is called but debugConfig is null."
        + " Initializing AbstractInterceptingComputation again...");
      initializeAbstractInterceptingComputation();
    }
    vertexId = vertex.getId();
    hasViolatedMsgValueConstraint = false;
    // A vertex should be debugged if:
    // 1) the user configures the superstep to be debugged;
    // 2) the user configues the vertex to be debugged; and
    // 3) we have already debugged less than a threshold of vertices in this
    // superstep.
    shouldDebugVertex = debugConfig.shouldDebugSuperstep(getSuperstep()) &&
      debugConfig.shouldDebugVertex(vertex) &&
      numVerticesLogged < NUM_VERTICES_TO_LOG;
    if (shouldDebugVertex) {
      giraphVertexScenarioWrapperForRegularTraces = getGiraphVertexScenario(
        vertex, vertex.getValue(), messages);
    }

    if (debugConfig.shouldCatchExceptions() ||
      debugConfig.shouldCheckVertexValueIntegrity() &&
      numVertexViolationsLogged < NUM_VIOLATIONS_TO_LOG ||
      debugConfig.shouldCheckMessageIntegrity() &&
      numMessageViolationsLogged < NUM_VIOLATIONS_TO_LOG) {
      previousVertexValue = DebuggerUtils.makeCloneOf(vertex.getValue(),
        getConf().getVertexValueClass());
    }
    return debugConfig.shouldCatchExceptions();
  }

  final protected void interceptComputeException(Vertex<I, V, E> vertex,
    Iterable<M1> messages, Exception e) throws IOException {
    LOG.info("Caught an exception. message: " + e.getMessage() +
      ". Saving a trace in HDFS.");
    GiraphVertexScenarioWrapper<I, V, E, M1, M2> giraphVertexScenarioWrapperForExceptionTrace = getGiraphVertexScenario(
      vertex, previousVertexValue, messages);
    ExceptionWrapper exceptionWrapper = new ExceptionWrapper(e.getMessage(),
      ExceptionUtils.getStackTrace(e));
    giraphVertexScenarioWrapperForExceptionTrace
      .setExceptionWrapper(exceptionWrapper);
    commonVertexMasterInterceptionUtil.saveScenarioWrapper(
      giraphVertexScenarioWrapperForExceptionTrace, DebuggerUtils
        .getFullTraceFileName(DebugTrace.VERTEX_EXCEPTION,
          commonVertexMasterInterceptionUtil.getJobId(), getSuperstep(),
          vertexId.toString()));
  }

  final protected void interceptComputeEnd(Vertex<I, V, E> vertex,
    Iterable<M1> messages) throws IOException {
    if (shouldDebugVertex) {
      commonVertexMasterInterceptionUtil.saveScenarioWrapper(
        giraphVertexScenarioWrapperForRegularTraces, DebuggerUtils
          .getFullTraceFileName(DebugTrace.VERTEX_REGULAR,
            commonVertexMasterInterceptionUtil.getJobId(), getSuperstep(),
            vertexId.toString()));
      numVerticesLogged++;
    }
    if (debugConfig.shouldCheckVertexValueIntegrity() &&
      numVertexViolationsLogged < NUM_VIOLATIONS_TO_LOG &&
      !debugConfig.isVertexValueCorrect(vertexId, vertex.getValue())) {
      initAndSaveGiraphVertexScenarioWrapper(vertex, messages,
        DebugTrace.INTEGRITY_VERTEX);
      numVertexViolationsLogged++;
    }
    if (hasViolatedMsgValueConstraint) {
      initAndSaveGiraphVertexScenarioWrapper(vertex, messages,
        DebugTrace.INTEGRITY_MESSAGE_SINGLE_VERTEX);
      numMessageViolationsLogged++;
    }
  }

  private void initAndSaveGiraphVertexScenarioWrapper(Vertex<I, V, E> vertex,
    Iterable<M1> messages, DebugTrace debugTrace) throws IOException {
    GiraphVertexScenarioWrapper<I, V, E, M1, M2> giraphVertexScenarioWrapper = getGiraphVertexScenario(
      vertex, previousVertexValue, messages);
    commonVertexMasterInterceptionUtil.saveScenarioWrapper(
      giraphVertexScenarioWrapper, DebuggerUtils.getFullTraceFileName(
        debugTrace, commonVertexMasterInterceptionUtil.getJobId(),
        getSuperstep(), vertexId.toString()));
  }

  // We pass the previous vertex value to assign as an argument because for some
  // traces we capture
  // the context lazily and store the previous value temporarily in an object.
  // In those cases
  // the previous value is not equal to the current value of the vertex. And
  // sometimes it is
  // equal to the current value.
  private GiraphVertexScenarioWrapper<I, V, E, M1, M2> getGiraphVertexScenario(
    Vertex<I, V, E> vertex, V previousVertexValueToAssign, Iterable<M1> messages)
    throws IOException {
    GiraphVertexScenarioWrapper<I, V, E, M1, M2> giraphVertexScenarioWrapper = new GiraphVertexScenarioWrapper(
      getActualTestedClass(), (Class<I>) vertexIdClazz,
      (Class<V>) vertexValueClazz, (Class<E>) edgeValueClazz,
      (Class<M1>) incomingMessageClazz, (Class<M2>) outgoingMessageClazz);
    VertexContextWrapper contextWrapper = giraphVertexScenarioWrapper.new VertexContextWrapper();
    giraphVertexScenarioWrapper.setContextWrapper(contextWrapper);
    giraphVertexScenarioWrapper.getContextWrapper()
      .setVertexValueBeforeWrapper(previousVertexValueToAssign);
    commonVertexMasterInterceptionUtil.initCommonVertexMasterContextWrapper(
      getConf(), getSuperstep(), getTotalNumVertices(), getTotalNumEdges());
    contextWrapper
      .setCommonVertexMasterContextWrapper(commonVertexMasterInterceptionUtil
        .getCommonVertexMasterContextWrapper());
    giraphVertexScenarioWrapper.getContextWrapper().setVertexIdWrapper(
      vertex.getId());
    Iterable<Edge<I, E>> returnVal = vertex.getEdges();
    for (Edge<I, E> edge : returnVal) {
      if (edge.getTargetVertexId() == null) {
        LOG.debug("the targetVertexId is null!!!");
      } else if (edge.getValue() == null) {
        LOG.debug("edge value is null!!! targetVertexId: " +
          edge.getTargetVertexId());
      }
      giraphVertexScenarioWrapper.getContextWrapper().addNeighborWrapper(
        edge.getTargetVertexId(), edge.getValue());
    }
    for (M1 message : messages) {
      giraphVertexScenarioWrapper.getContextWrapper()
        .addIncomingMessageWrapper(message);
    }
    giraphVertexScenarioWrapper.getContextWrapper().setVertexValueAfterWrapper(
      vertex.getValue());
    return giraphVertexScenarioWrapper;
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
      giraphVertexScenarioWrapperForRegularTraces.getContextWrapper()
        .addOutgoingMessageWrapper(id, message);
    }
    if (debugConfig.shouldCheckMessageIntegrity() &&
      !debugConfig.isMessageCorrect(vertexId, id, message) &&
      numMessageViolationsLogged < NUM_VIOLATIONS_TO_LOG) {
      msgIntegrityViolationWrapper.addMsgWrapper(vertexId, id, message);
      hasViolatedMsgValueConstraint = true;
      numMessageViolationsLogged++;
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
      interceptMessageAndCheckIntegrityIfNecessary(edge.getTargetVertexId(),
        message);
    }
    super.sendMessageToAllEdges(vertex, message);
  }

  final protected void interceptPreSuperstepBegin() {
    numVerticesLogged = 0;
    numVertexViolationsLogged = 0;
    numMessageViolationsLogged = 0;
    if (debugConfig.shouldCheckMessageIntegrity()) {
      LOG.info("creating a msgIntegrityViolationWrapper. superstepNo: " +
        getSuperstep());
      msgIntegrityViolationWrapper = new MsgIntegrityViolationWrapper<>(
        (Class<I>) vertexIdClazz, (Class<M2>) outgoingMessageClazz);
      msgIntegrityViolationWrapper.setSuperstepNo(getSuperstep());
    }
    if (debugConfig.shouldCheckVertexValueIntegrity()) {
      LOG.info("creating a vertexValueViolationWrapper. superstepNo: " +
        getSuperstep());
    }
  }

  final protected void interceptPostSuperstepEnd() {
    if (debugConfig.shouldCheckMessageIntegrity() &&
      msgIntegrityViolationWrapper.numMsgWrappers() > 0) {
      commonVertexMasterInterceptionUtil.saveScenarioWrapper(
        msgIntegrityViolationWrapper, DebuggerUtils
          .getMessageIntegrityAllTraceFullFileName(getSuperstep(),
            commonVertexMasterInterceptionUtil.getJobId(), UUID.randomUUID()
              .toString()));
    }
  }

  public abstract Class<? extends Computation<I, V, E, ? extends Writable, ? extends Writable>> getActualTestedClass();

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    A retVal = super.<A> getAggregatedValue(name);
    commonVertexMasterInterceptionUtil.addAggregatedValueIfNotExists(name,
      retVal);
    return retVal;
  }
}
