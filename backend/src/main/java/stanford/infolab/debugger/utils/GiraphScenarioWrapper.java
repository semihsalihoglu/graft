package stanford.infolab.debugger.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.protobuf.GeneratedMessage;

import stanford.infolab.debugger.GiraphAggregator.AggregatedValue;
import stanford.infolab.debugger.Scenario.GiraphScenario;
import stanford.infolab.debugger.Scenario.GiraphScenario.Context;
import stanford.infolab.debugger.Scenario.GiraphScenario.ContextOrBuilder;
import stanford.infolab.debugger.Scenario.GiraphScenario.Context.Neighbor;
import stanford.infolab.debugger.Scenario.GiraphScenario.Context.OutMsg;
import stanford.infolab.debugger.utils.AggregatedValueWrapper;
import stanford.infolab.debugger.utils.GiraphScenarioWrapper.ContextWrapper.OutgoingMessageWrapper;

/**
 * A wrapper class around the contents of scenario.proto. In scenario.proto most
 * things are stored as serialized byte arrays and this class gives them access
 * through the java classes that those byte arrays serialize.
 * 
 * @param <I>
 *          vertex ID class.
 * @param <V>
 *          vertex value class.
 * @param <E>
 *          edge value class.
 * @param <M1>
 *          incoming message class.
 * @param <M2>
 *          outgoing message class.
 * @author Brian Truong
 */
@SuppressWarnings("rawtypes")
public class GiraphScenarioWrapper<I extends WritableComparable, V extends Writable,
  E extends Writable, M1 extends Writable, M2 extends Writable> extends BaseScenarioAndIntegrityWrapper<I> {

  private Class<?> classUnderTest;
  private Class<V> vertexValueClass;
  private Class<E> edgeValueClass;
  private Class<M1> incomingMessageClass;
  private Class<M2> outgoingMessageClass;

  private ContextWrapper contextWrapper = null;
  private ExceptionWrapper exceptionWrapper = null;
  private ImmutableClassesGiraphConfiguration<I, V, E> immutableClassesConfig = null;
  
  // Empty constructor to be used for loading from HDFS.
  public GiraphScenarioWrapper() {}

  public GiraphScenarioWrapper(Class<? extends Computation<I, V, E, M1, M2>> classUnderTest,
    Class<I> vertexIdClass, Class<V> vertexValueClass, Class<E> edgeValueClass,
    Class<M1> incomingMessageClass, Class<M2> outgoingMessageClass) {
    initialize(classUnderTest, vertexIdClass, vertexValueClass, edgeValueClass, incomingMessageClass,
      outgoingMessageClass);
  }
  
  private void initialize(Class<?> classUnderTest,
    Class<I> vertexIdClass, Class<V> vertexValueClass, Class<E> edgeValueClass,
    Class<M1> incomingMessageClass, Class<M2> outgoingMessageClass) {
    super.initialize(vertexIdClass);
    this.classUnderTest = classUnderTest;
    this.vertexValueClass = vertexValueClass;
    this.edgeValueClass = edgeValueClass;
    this.incomingMessageClass = incomingMessageClass;
    this.outgoingMessageClass = outgoingMessageClass;    
  }

  public Class<?> getClassUnderTest() {
    return classUnderTest;
  }

  public Class<V> getVertexValueClass() {
    return vertexValueClass;
  }

  public Class<E> getEdgeValueClass() {
    return edgeValueClass;
  }

  public Class<M1> getIncomingMessageClass() {
    return incomingMessageClass;
  }

  public Class<M2> getOutgoingMessageClass() {
    return outgoingMessageClass;
  }

  public ContextWrapper getContextWrapper() {
    return contextWrapper;
  }

  public ImmutableClassesGiraphConfiguration<I, V, E> getConfig() {
    return immutableClassesConfig;
  }

  public void setContextWrapper(ContextWrapper context) {
    this.contextWrapper = context;
  }

  public boolean hasExceptionWrapper() {
    return exceptionWrapper != null;
  }

  public ExceptionWrapper getExceptionWrapper() {
    return exceptionWrapper;
  }

  public void setExceptionWrapper(ExceptionWrapper exceptionWrapper) {
    this.exceptionWrapper = exceptionWrapper;
  }

  public void setConfig(ImmutableClassesGiraphConfiguration<I, V, E> immutableClassesConfig) {
    this.immutableClassesConfig = immutableClassesConfig;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\nconfig: " + immutableClassesConfig.toString());
    stringBuilder.append("\nclassUnderTest: " + getClassUnderTest().getCanonicalName());
    stringBuilder.append("\nvertexValueClass: " + getVertexValueClass().getCanonicalName());
    stringBuilder.append("\nincomingMessageClass: " + getIncomingMessageClass().getCanonicalName());
    stringBuilder.append("\noutgoingMessageClass: " + getOutgoingMessageClass().getCanonicalName());
    stringBuilder.append("\n" + contextWrapper.toString());
    stringBuilder.append("\nhasExceptionWrapper: " + hasExceptionWrapper());
    if (hasExceptionWrapper()) {
      stringBuilder.append("\n" + exceptionWrapper.toString());
    }
    return stringBuilder.toString();
  }

  // NOTE: We actually do not need to wrap GiraphScenario.Exception because it
  // doesn't contain
  // any typed fields. We do it here only to be consistent with the rest of the
  // class.
  public static class ExceptionWrapper {
    private String errorMessage = "";
    private String stackTrace = "";

    public ExceptionWrapper(String errorMessage, String stackTrace) {
      this.errorMessage = errorMessage;
      this.stackTrace = stackTrace;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("errorMessage: " + getErrorMessage());
      stringBuilder.append("\nstackTrace: " + getStackTrace());
      return stringBuilder.toString();
    }

    public String getErrorMessage() {
      return "" + errorMessage;
    }

    public String getStackTrace() {
      return "" + stackTrace;
    }
  }

  public class ContextWrapper {
    private long superstepNo;
    private long totalNumVertices;
    private long totalNumEdges;
    private I vertexId;
    private V vertexValueBefore;
    private V vertexValueAfter;
    private ArrayList<M1> inMsgs;
    private ArrayList<NeighborWrapper> neighbors;
    private ArrayList<OutgoingMessageWrapper> outMsgs;
    private ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers;
    
    public ContextWrapper() {
      reset();
    }

    public void reset() {
      this.superstepNo = -1;
      this.totalNumVertices = -1;
      this.totalNumEdges = -1;
      this.vertexId = null;
      this.vertexValueBefore = null;
      this.vertexValueAfter = null;
      this.inMsgs = new ArrayList<M1>();
      this.neighbors = new ArrayList<NeighborWrapper>();
      this.outMsgs = new ArrayList<OutgoingMessageWrapper>();
      this.previousAggregatedValueWrappers = new ArrayList<>();
    }

    public long getSuperstepNoWrapper() {
      return superstepNo;
    }

    public long getTotalNumVerticesWrapper() {
      return totalNumVertices;
    }

    public long getTotalNumEdgesWrapper() {
      return totalNumEdges;
    }

    public void setSuperstepNoWrapper(long superstepNo) {
      this.superstepNo = superstepNo;
    }

    public void setTotalNumVerticesWrapper(long totalNumVertices) {
      this.totalNumVertices = totalNumVertices;
    }

    public void setTotalNumEdgesWrapper(long totalNumEdges) {
      this.totalNumEdges = totalNumEdges;
    }

    public I getVertexIdWrapper() {
      return vertexId;
    }

    public void setVertexIdWrapper(I vertexId) {
      this.vertexId = vertexId;
    }

    public V getVertexValueBeforeWrapper() {
      return vertexValueBefore;
    }

    public V getVertexValueAfterWrapper() {
      return vertexValueAfter;
    }
    
    public void setVertexValueBeforeWrapper(V vertexValueBefore) {
      // Because Giraph does not create new objects for writables, we need
      // to make a clone them to get a copy of the objects. Otherwise, if
      // we call setVertexValueBeforeWrapper and then setVertexValueAfterWrapper
      // both of our copies end up pointing to the same object (in this case to
      // the value passed to setVertexValueAfterWrapper, because it was called
      // later).
      this.vertexValueBefore = makeCloneOf(vertexValueBefore, vertexValueClass);
    }

    public void setVertexValueAfterWrapper(V vertexValueAfter) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      this.vertexValueAfter = makeCloneOf(vertexValueAfter, vertexValueClass);
    }

    public void addIncomingMessageWrapper(M1 message) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      inMsgs.add(makeCloneOf(message, incomingMessageClass));
    }

    public Collection<M1> getIncomingMessageWrappers() {
      return inMsgs;
    }
    
    public void addPreviousAggregatedValue(AggregatedValueWrapper previousAggregatedValueWrapper) {
      this.previousAggregatedValueWrappers.add(previousAggregatedValueWrapper);
    }

    public void setPreviousAggregatedValues(
      ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers) {
      this.previousAggregatedValueWrappers = previousAggregatedValueWrappers;
    }

    public Collection<AggregatedValueWrapper> getPreviousAggregatedValues() {
      return previousAggregatedValueWrappers;
    }

    public void addOutgoingMessageWrapper(I receiverId, M2 message) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      outMsgs.add(new OutgoingMessageWrapper(makeCloneOf(receiverId, vertexIdClass), makeCloneOf(
        message, outgoingMessageClass)));
    }

    public Collection<OutgoingMessageWrapper> getOutgoingMessageWrappers() {
      return outMsgs;
    }

    public void addNeighborWrapper(I neighborId, E edgeValue) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      neighbors.add(new NeighborWrapper(makeCloneOf(neighborId, vertexIdClass), makeCloneOf(
        edgeValue, edgeValueClass)));
    }

    public Collection<NeighborWrapper> getNeighborWrappers() {
      return neighbors;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("superstepNo: " + getSuperstepNoWrapper());
      stringBuilder.append("\nvertexId: " + getVertexIdWrapper());
      stringBuilder.append("\ntotalNumVertices: " + totalNumVertices);
      stringBuilder.append("\ntotalNumEdges: " + totalNumEdges);
      stringBuilder.append("\nvertexValueBefore: " + getVertexValueBeforeWrapper());
      stringBuilder.append("\nvertexValueAfter: " + getVertexValueAfterWrapper());
      stringBuilder.append("\nnumNeighbors: " + getNeighborWrappers().size());

      for (NeighborWrapper neighborWrapper : getNeighborWrappers()) {
        stringBuilder.append("\n" + neighborWrapper.toString());
      }

      for (M1 incomingMesage : getIncomingMessageWrappers()) {
        stringBuilder.append("\nincoming message: " + incomingMesage);
      }

      for (OutgoingMessageWrapper outgoingMessage : getOutgoingMessageWrappers()) {
        stringBuilder.append("\n" + outgoingMessage);
      }
      stringBuilder.append("\nnumAggregators: " + getPreviousAggregatedValues().size());
      for (AggregatedValueWrapper aggregatedValueWrapper : getPreviousAggregatedValues()) {
        stringBuilder.append("\n" + aggregatedValueWrapper);
      }
      return stringBuilder.toString();
    }

    /**
     * Wrapper around scenario.giraphscenerio.neighbor (in scenario.proto).
     * 
     * @author Brian Truong
     */
    public class NeighborWrapper {

      private I nbrId;
      private E edgeValue;

      public NeighborWrapper(I nbrId, E edgeValue) {
        this.nbrId = nbrId;
        this.edgeValue = edgeValue;
      }

      public I getNbrId() {
        return nbrId;
      }

      public E getEdgeValue() {
        return edgeValue;
      }

      @Override
      public String toString() {
        return "neighbor: nbrId: " + nbrId + " edgeValue: " + edgeValue;
      }
    }

    public class OutgoingMessageWrapper {
      public I destinationId;
      public M2 message;

      public OutgoingMessageWrapper(I destinationId, M2 message) {
        this.destinationId = destinationId;
        this.message = message;
      }

      @Override
      public String toString() {
        return "outgoingMessage: destinationId: " + destinationId + " message: " + message;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    GiraphScenario giraphScenario = (GiraphScenario) generatedMessage;
    Class<?> clazz = Class.forName(giraphScenario.getClassUnderTest());
    Class<?> classUnderTest = 
      castClassToUpperBound(clazz,
        Computation.class);

    Class<I> vertexIdClass = (Class<I>) castClassToUpperBound(
      Class.forName(giraphScenario.getVertexIdClass()), WritableComparable.class);

    Class<V> vertexValueClass = (Class<V>) castClassToUpperBound(
      Class.forName(giraphScenario.getVertexValueClass()), Writable.class);

    Class<E> edgeValueClass = (Class<E>) castClassToUpperBound(
      Class.forName(giraphScenario.getEdgeValueClass()), Writable.class);

    Class<M1> incomingMessageClass = (Class<M1>) castClassToUpperBound(
      Class.forName(giraphScenario.getIncomingMessageClass()), Writable.class);

    Class<M2> outgoingMessageClass = (Class<M2>) castClassToUpperBound(
      Class.forName(giraphScenario.getOutgoingMessageClass()), Writable.class);

    initialize(classUnderTest, vertexIdClass, vertexValueClass, edgeValueClass, incomingMessageClass,
      outgoingMessageClass);

    GiraphConfiguration config = new GiraphConfiguration();
    fromByteString(giraphScenario.getConf(), config);
    ImmutableClassesGiraphConfiguration<I, V, E> immutableClassesGiraphConfiguration =
      new ImmutableClassesGiraphConfiguration<>(config);
    immutableClassesGiraphConfiguration.setComputationClass(
      (Class<? extends Computation>) classUnderTest);
    setConfig(immutableClassesGiraphConfiguration);
    ContextOrBuilder context = giraphScenario.getContextOrBuilder();
    GiraphScenarioWrapper<I, V, E, M1, M2>.ContextWrapper contextWrapper =
      this.new ContextWrapper();
    contextWrapper.setSuperstepNoWrapper(context.getSuperstepNo());
    contextWrapper.setTotalNumVerticesWrapper(context.getTotalNumVertices());
    contextWrapper.setTotalNumEdgesWrapper(context.getTotalNumEdges());
    I vertexId = newInstance(vertexIdClass);
    fromByteString(context.getVertexId(), vertexId);
    contextWrapper.setVertexIdWrapper(vertexId);

    V vertexValue = newInstance(vertexValueClass);
    fromByteString(context.getVertexValueBefore(), vertexValue);
    contextWrapper.setVertexValueBeforeWrapper(vertexValue);
    if (context.hasVertexValueAfter()) {
      fromByteString(context.getVertexValueAfter(), vertexValue);
      contextWrapper.setVertexValueAfterWrapper(vertexValue);
    }

    for (Neighbor neighbor : context.getNeighborList()) {
      I neighborId = newInstance(vertexIdClass);
      fromByteString(neighbor.getNeighborId(), neighborId);

      E edgeValue;
      if (neighbor.hasEdgeValue()) {
        edgeValue = newInstance(edgeValueClass);
        fromByteString(neighbor.getEdgeValue(), edgeValue);
      } else {
        edgeValue = null;
      }
      contextWrapper.addNeighborWrapper(neighborId, edgeValue);
    }
    for (int i = 0; i < context.getInMessageCount(); i++) {
      M1 msg = newInstance(incomingMessageClass);
      fromByteString(context.getInMessage(i), msg);
      contextWrapper.addIncomingMessageWrapper(msg);
    }

    for (OutMsg outmsg : context.getOutMessageList()) {
      I destinationId = newInstance(vertexIdClass);
      fromByteString(outmsg.getDestinationId(), destinationId);
      M2 msg = newInstance(outgoingMessageClass);
      fromByteString(outmsg.getMsgData(), msg);
      contextWrapper.addOutgoingMessageWrapper(destinationId, msg);
    }

    for (AggregatedValue previousAggregatedValueProto : context.getPreviousAggregatedValueList()) {
      AggregatedValueWrapper aggregatedValueWrapper = new AggregatedValueWrapper();
      aggregatedValueWrapper.loadFromProto(previousAggregatedValueProto);
      contextWrapper.addPreviousAggregatedValue(aggregatedValueWrapper);
    }

    if (giraphScenario.hasException()) {
      ExceptionWrapper exceptionWrapper = new ExceptionWrapper(giraphScenario.getException()
        .getMessage(), giraphScenario.getException().getStackTrace());
      setExceptionWrapper(exceptionWrapper);
    }
    
    setContextWrapper(contextWrapper);
  }

  @Override
  public  GeneratedMessage buildProtoObject() {
    GiraphScenario.Builder giraphScenarioBuilder = GiraphScenario.newBuilder();
    giraphScenarioBuilder.setClassUnderTest(getClassUnderTest().getName());
    giraphScenarioBuilder.setVertexIdClass(getVertexIdClass().getName());
    giraphScenarioBuilder.setVertexValueClass(getVertexValueClass().getName());
    giraphScenarioBuilder.setEdgeValueClass(getEdgeValueClass().getName());
    giraphScenarioBuilder.setIncomingMessageClass(getIncomingMessageClass().getName());
    giraphScenarioBuilder.setOutgoingMessageClass(getOutgoingMessageClass().getName());
    giraphScenarioBuilder.setConf(toByteString(immutableClassesConfig));

    GiraphScenarioWrapper<I, V, E, M1, M2>.ContextWrapper contextWrapper = getContextWrapper();
    Context.Builder contextBuilder = Context.newBuilder();
    contextBuilder.setSuperstepNo(contextWrapper.getSuperstepNoWrapper())
                  .setVertexId(toByteString(contextWrapper.getVertexIdWrapper()))
                  .setTotalNumVertices(contextWrapper.getTotalNumVerticesWrapper())
                  .setTotalNumEdges(contextWrapper.getTotalNumEdgesWrapper());
    if (contextWrapper.getVertexValueBeforeWrapper() != null) {
      contextBuilder.setVertexValueBefore(
        toByteString(contextWrapper.getVertexValueBeforeWrapper()));
    }
    if (contextWrapper.getVertexValueAfterWrapper() != null) {
      contextBuilder.setVertexValueAfter(toByteString(contextWrapper.getVertexValueAfterWrapper()));
    }

    for (GiraphScenarioWrapper<I, V, E, M1, M2>.ContextWrapper.NeighborWrapper neighbor : contextWrapper
      .getNeighborWrappers()) {
      Neighbor.Builder neighborBuilder = Neighbor.newBuilder();
      neighborBuilder.setNeighborId(toByteString(neighbor.getNbrId()));
      E edgeValue = neighbor.getEdgeValue();
      if (edgeValue != null) {
        neighborBuilder.setEdgeValue(toByteString(edgeValue));
      } else {
        neighborBuilder.clearEdgeValue();
      }
      contextBuilder.addNeighbor(neighborBuilder.build());
    }

    for (M1 msg : contextWrapper.getIncomingMessageWrappers()) {
      contextBuilder.addInMessage(toByteString(msg));
    }

    for (OutgoingMessageWrapper msg : contextWrapper.getOutgoingMessageWrappers()) {
      OutMsg.Builder outMsgBuilder = OutMsg.newBuilder();
      outMsgBuilder.setMsgData(toByteString(msg.message));
      outMsgBuilder.setDestinationId(toByteString(msg.destinationId));
      contextBuilder.addOutMessage(outMsgBuilder);
    }

    for (AggregatedValueWrapper aggregatedValueWrapper :
      contextWrapper.getPreviousAggregatedValues()) {
      contextBuilder.addPreviousAggregatedValue(
        (AggregatedValue) aggregatedValueWrapper.buildProtoObject());
    }

    giraphScenarioBuilder.setContext(contextBuilder.build());
    if (hasExceptionWrapper()) {
      GiraphScenario.Exception.Builder exceptionBuilder = GiraphScenario.Exception.newBuilder();
      exceptionBuilder.setMessage(getExceptionWrapper().getErrorMessage());
      exceptionBuilder.setStackTrace(getExceptionWrapper().getStackTrace());
      giraphScenarioBuilder.setException(exceptionBuilder.build());
    }
    GiraphScenario giraphScenario = giraphScenarioBuilder.build();
    return giraphScenario;
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return GiraphScenario.parseFrom(inputStream);
  }
}
