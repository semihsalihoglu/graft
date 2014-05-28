package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.debugger.Scenario.CommonVertexMasterContext;
import org.apache.giraph.debugger.Scenario.Exception;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext.Neighbor;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext.OutgoingMessage;
import org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexScenarioClasses;
import org.apache.giraph.graph.Computation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around {@link org.apache.giraph.debugger.Scenario.GiraphVertexScenario}
 * protocol buffer. In {@link org.apache.giraph.debugger.Scenario.GiraphVertexScenario} most fields
 * are stored as serialized byte arrays and this class gives them access through the java classes
 * that those byte arrays serialize.
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
public class GiraphVertexScenarioWrapper<I extends WritableComparable, V extends Writable,
  E extends Writable, M1 extends Writable, M2 extends Writable> extends BaseWrapper {

  private VertexScenarioClassesWrapper vertexScenarioClassesWrapper = null;
  private VertexContextWrapper contextWrapper = null;
  private ExceptionWrapper exceptionWrapper = null;

  // Empty constructor to be used for loading from HDFS.
  public GiraphVertexScenarioWrapper() {}

  public GiraphVertexScenarioWrapper(Class<? extends Computation<I, V, E, M1, M2>> classUnderTest,
    Class<I> vertexIdClass, Class<V> vertexValueClass, Class<E> edgeValueClass,
    Class<M1> incomingMessageClass, Class<M2> outgoingMessageClass) {
    this.vertexScenarioClassesWrapper = new VertexScenarioClassesWrapper(classUnderTest,
      vertexIdClass, vertexValueClass, edgeValueClass, incomingMessageClass, outgoingMessageClass);
    this.contextWrapper = new VertexContextWrapper();
  }

  public VertexContextWrapper getContextWrapper() {
    return contextWrapper;
  }

  public void setContextWrapper(VertexContextWrapper contextWrapper) {
    this.contextWrapper = contextWrapper;
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

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\n" + vertexScenarioClassesWrapper.toString());
    stringBuilder.append("\n" + contextWrapper.toString());
    stringBuilder.append("\nhasExceptionWrapper: " + hasExceptionWrapper());
    if (hasExceptionWrapper()) {
      stringBuilder.append("\n" + exceptionWrapper.toString());
    }
    return stringBuilder.toString();
  }

  /**
   * Wrapper class around {@link org.apache.giraph.debugger.Scenario.GiraphVertexScenario.VertexContext}
   * protocol buffer.
   *
   * @author semihsalihoglu
   */
  public class VertexContextWrapper extends BaseWrapper {
    private CommonVertexMasterContextWrapper commonVertexMasterContextWrapper;
    private I vertexIdWrapper;
    private V vertexValueBeforeWrapper;
    private V vertexValueAfterWrapper;
    private ArrayList<M1> inMsgsWrapper;
    private ArrayList<NeighborWrapper> neighborsWrapper;
    private ArrayList<OutgoingMessageWrapper> outMsgsWrapper;

    public VertexContextWrapper() {
      reset();
    }

    public void reset() {
      this.commonVertexMasterContextWrapper = new CommonVertexMasterContextWrapper();
      this.vertexIdWrapper = null;
      this.vertexValueBeforeWrapper = null;
      this.vertexValueAfterWrapper = null;
      this.inMsgsWrapper = new ArrayList<M1>();
      this.neighborsWrapper = new ArrayList<NeighborWrapper>();
      this.outMsgsWrapper = new ArrayList<OutgoingMessageWrapper>();
    }

    public CommonVertexMasterContextWrapper getCommonVertexMasterContextWrapper() {
      return commonVertexMasterContextWrapper;
    }

    public void setCommonVertexMasterContextWrapper(CommonVertexMasterContextWrapper commonVertexMasterContextWrapper) {
      this.commonVertexMasterContextWrapper = commonVertexMasterContextWrapper;
    }

    public I getVertexIdWrapper() {
      return vertexIdWrapper;
    }

    public void setVertexIdWrapper(I vertexId) {
      this.vertexIdWrapper = vertexId;
    }

    public V getVertexValueBeforeWrapper() {
      return vertexValueBeforeWrapper;
    }

    public V getVertexValueAfterWrapper() {
      return vertexValueAfterWrapper;
    }
    
    public void setVertexValueBeforeWrapper(V vertexValueBefore) {
      // Because Giraph does not create new objects for writables, we need
      // to make a clone them to get a copy of the objects. Otherwise, if
      // we call setVertexValueBeforeWrapper and then setVertexValueAfterWrapper
      // both of our copies end up pointing to the same object (in this case to
      // the value passed to setVertexValueAfterWrapper, because it was called
      // later).
      this.vertexValueBeforeWrapper = makeCloneOf(vertexValueBefore,
        getVertexScenarioClassesWrapper().vertexValueClass);
    }

    public void setVertexValueAfterWrapper(V vertexValueAfter) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      this.vertexValueAfterWrapper = makeCloneOf(vertexValueAfter,
        getVertexScenarioClassesWrapper().vertexValueClass);
    }

    public void addIncomingMessageWrapper(M1 message) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      inMsgsWrapper.add(makeCloneOf(message, getVertexScenarioClassesWrapper().incomingMessageClass));
    }

    public Collection<M1> getIncomingMessageWrappers() {
      return inMsgsWrapper;
    }

    public void addOutgoingMessageWrapper(I receiverId, M2 message) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      outMsgsWrapper.add(new OutgoingMessageWrapper(makeCloneOf(receiverId,
        getVertexScenarioClassesWrapper().vertexIdClass),
        makeCloneOf(message, getVertexScenarioClassesWrapper().outgoingMessageClass)));
    }

    public Collection<OutgoingMessageWrapper> getOutgoingMessageWrappers() {
      return outMsgsWrapper;
    }

    public void addNeighborWrapper(I neighborId, E edgeValue) {
      // See the explanation for making a clone inside
      // setVertexValueBeforeWrapper
      neighborsWrapper.add(new NeighborWrapper(
        makeCloneOf(neighborId, getVertexScenarioClassesWrapper().vertexIdClass),
        makeCloneOf(edgeValue, getVertexScenarioClassesWrapper().edgeValueClass)));
    }

    public Collection<NeighborWrapper> getNeighborWrappers() {
      return neighborsWrapper;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(commonVertexMasterContextWrapper.toString());
      stringBuilder.append("\nvertexId: " + getVertexIdWrapper());
      stringBuilder.append("\nvertexValueBefore: " + getVertexValueBeforeWrapper());
      stringBuilder.append("\nvertexValueAfter: " + getVertexValueAfterWrapper());
      stringBuilder.append("\nnumNeighbors: " + getNeighborWrappers().size());

      for (NeighborWrapper neighborWrapper : getNeighborWrappers()) {
        stringBuilder.append("\n" + neighborWrapper.toString());
      }

      for (M1 incomingMesage : getIncomingMessageWrappers()) {
        stringBuilder.append("\nincoming message: " + incomingMesage);
      }

      stringBuilder.append("\nnumOutgoingMessages: " + getOutgoingMessageWrappers().size());
      for (OutgoingMessageWrapper outgoingMessageWrapper : getOutgoingMessageWrappers()) {
        stringBuilder.append("\n" + outgoingMessageWrapper);
      }
      return stringBuilder.toString();
    }

    /**
     * Wrapper around scenario.giraphscenerio.neighbor (in scenario.proto).
     * 
     * @author Brian Truong
     */
    public class NeighborWrapper extends BaseWrapper {

      private I nbrId;
      private E edgeValue;

      public NeighborWrapper(I nbrId, E edgeValue) {
        this.nbrId = nbrId;
        this.edgeValue = edgeValue;
      }

      public NeighborWrapper() {}

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

      @Override
      public GeneratedMessage buildProtoObject() {
        Neighbor.Builder neighborBuilder = Neighbor.newBuilder();
        neighborBuilder.setNeighborId(toByteString(nbrId));
        if (edgeValue != null) {
          neighborBuilder.setEdgeValue(toByteString(edgeValue));
        } else {
          neighborBuilder.clearEdgeValue();
        }
        return neighborBuilder.build();
      }

      @Override
      public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
        return Neighbor.parseFrom(inputStream);
      }

      @Override
      public void loadFromProto(GeneratedMessage protoObject) throws ClassNotFoundException,
        IOException, InstantiationException, IllegalAccessException {
        Neighbor neighbor = (Neighbor) protoObject;
        this.nbrId = newInstance(vertexScenarioClassesWrapper.vertexIdClass);
        fromByteString(neighbor.getNeighborId(), this.nbrId);

        if (neighbor.hasEdgeValue()) {
          this.edgeValue = newInstance(vertexScenarioClassesWrapper.edgeValueClass);
          fromByteString(neighbor.getEdgeValue(), this.edgeValue);
        } else {
          this.edgeValue = null;
        }
      }
    }

    public class OutgoingMessageWrapper extends BaseWrapper {
      public I destinationId;
      public M2 message;

      public OutgoingMessageWrapper(I destinationId, M2 message) {
        this.destinationId = destinationId;
        this.message = message;
      }
      
      public OutgoingMessageWrapper() {}

      public I getDestinationId() {
        return destinationId;
      }
      
      public M2 getMessage() {
        return message;
      }

      @Override
      public String toString() {
        return "outgoingMessage: destinationId: " + destinationId + " message: " + message;
      }
      
      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((destinationId == null) ? 0 : destinationId.hashCode());
        result = prime * result + ((message == null) ? 0 : message.hashCode());
        return result;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj)
          return true;
        if (obj == null)
          return false;
        if (getClass() != obj.getClass())
          return false;
        @SuppressWarnings("unchecked")
        OutgoingMessageWrapper other = (OutgoingMessageWrapper) obj;
        if (destinationId == null) {
          if (other.destinationId != null)
            return false;
        } else if (!destinationId.equals(other.destinationId))
          return false;
        if (message == null) {
          if (other.message != null)
            return false;
        } else if (!message.equals(other.message))
          return false;
        return true;
      }

      @Override
      public GeneratedMessage buildProtoObject() {
        OutgoingMessage.Builder outgoingMessageBuilder = OutgoingMessage.newBuilder();
        outgoingMessageBuilder.setMsgData(toByteString(this.message));
        outgoingMessageBuilder.setDestinationId(toByteString(this.destinationId));
        return outgoingMessageBuilder.build();
      }

      @Override
      public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
        return OutgoingMessage.parseFrom(inputStream);
      }

      @Override
      public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
        IOException, InstantiationException, IllegalAccessException {
        OutgoingMessage outgoingMessageProto = (OutgoingMessage) generatedMessage;
        this.destinationId = newInstance(getVertexScenarioClassesWrapper().vertexIdClass);
        fromByteString(outgoingMessageProto.getDestinationId(), destinationId);
        this.message = newInstance(getVertexScenarioClassesWrapper().outgoingMessageClass);
        fromByteString(outgoingMessageProto.getMsgData(), this.message);
      }
    }

    @Override
    public GeneratedMessage buildProtoObject() {
      VertexContext.Builder contextBuilder = VertexContext.newBuilder();
      contextBuilder.setCommonContext((CommonVertexMasterContext)
        commonVertexMasterContextWrapper.buildProtoObject());
      contextBuilder.setVertexId(toByteString(vertexIdWrapper));
      if (vertexValueBeforeWrapper != null) {
        contextBuilder.setVertexValueBefore(toByteString(vertexValueBeforeWrapper));
      }
      if (vertexValueAfterWrapper != null) {
        contextBuilder.setVertexValueAfter(toByteString(vertexValueAfterWrapper));
      }

      for (GiraphVertexScenarioWrapper<I, V, E, M1, M2>.VertexContextWrapper.NeighborWrapper neighborWrapper : 
        neighborsWrapper) {
        contextBuilder.addNeighbor((Neighbor) neighborWrapper.buildProtoObject());
      }

      for (M1 msg : inMsgsWrapper) {
        contextBuilder.addInMessage(toByteString(msg));
      }

      for (OutgoingMessageWrapper outgoingMessageWrapper : outMsgsWrapper) {
        contextBuilder.addOutMessage(
          (OutgoingMessage) outgoingMessageWrapper.buildProtoObject());
      }

      return contextBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
      return VertexContext.parseFrom(inputStream);
    }

    @Override
    public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
      IOException, InstantiationException, IllegalAccessException {
      VertexContext context = (VertexContext) generatedMessage;

      CommonVertexMasterContextWrapper commonVertexMasterContextWrapper =
        new CommonVertexMasterContextWrapper();
      commonVertexMasterContextWrapper.loadFromProto(context.getCommonContext());
      this.commonVertexMasterContextWrapper = commonVertexMasterContextWrapper;

      I vertexId = newInstance(getVertexScenarioClassesWrapper().vertexIdClass);
      fromByteString(context.getVertexId(), vertexId);
      this.vertexIdWrapper = vertexId;

      V vertexValueBefore = newInstance(getVertexScenarioClassesWrapper().vertexValueClass);
      fromByteString(context.getVertexValueBefore(), vertexValueBefore);
      this.vertexValueBeforeWrapper = vertexValueBefore;
      if (context.hasVertexValueAfter()) {
        V vertexValueAfter = newInstance(getVertexScenarioClassesWrapper().vertexValueClass);
        fromByteString(context.getVertexValueAfter(), vertexValueAfter);
        this.vertexValueAfterWrapper = vertexValueAfter;
      }

      for (Neighbor neighbor : context.getNeighborList()) {
        NeighborWrapper neighborWrapper = new NeighborWrapper();
        neighborWrapper.loadFromProto(neighbor);
        this.neighborsWrapper.add(neighborWrapper);
      }
      for (int i = 0; i < context.getInMessageCount(); i++) {
        M1 msg = newInstance(getVertexScenarioClassesWrapper().incomingMessageClass);
        fromByteString(context.getInMessage(i), msg);
        this.addIncomingMessageWrapper(msg);
      }

      for (OutgoingMessage outgoingMessageProto : context.getOutMessageList()) {
        OutgoingMessageWrapper outgoingMessageWrapper = new OutgoingMessageWrapper();
        outgoingMessageWrapper.loadFromProto(outgoingMessageProto);
        this.outMsgsWrapper.add(outgoingMessageWrapper);
      }
    }
  }

  public class VertexScenarioClassesWrapper extends BaseScenarioAndIntegrityWrapper<I> {
    private Class<?> classUnderTest;
    private Class<V> vertexValueClass;
    private Class<E> edgeValueClass;
    private Class<M1> incomingMessageClass;
    private Class<M2> outgoingMessageClass;

    public VertexScenarioClassesWrapper() {}

    public VertexScenarioClassesWrapper(
      Class<? extends Computation<I, V, E, M1, M2>> classUnderTest,
      Class<I> vertexIdClass, Class<V> vertexValueClass, Class<E> edgeValueClass,
      Class<M1> incomingMessageClass, Class<M2> outgoingMessageClass) {
      super(vertexIdClass);
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

    @Override
    public GeneratedMessage buildProtoObject() {
      VertexScenarioClasses.Builder vertexScenarioClassesBuilder =
        VertexScenarioClasses.newBuilder();
      vertexScenarioClassesBuilder.setClassUnderTest(getClassUnderTest().getName());
      vertexScenarioClassesBuilder.setVertexIdClass(getVertexIdClass().getName());
      vertexScenarioClassesBuilder.setVertexValueClass(getVertexValueClass().getName());
      vertexScenarioClassesBuilder.setEdgeValueClass(getEdgeValueClass().getName());
      vertexScenarioClassesBuilder.setIncomingMessageClass(getIncomingMessageClass().getName());
      vertexScenarioClassesBuilder.setOutgoingMessageClass(getOutgoingMessageClass().getName());
      return vertexScenarioClassesBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
      return VertexScenarioClasses.parseFrom(inputStream);
    }

    @Override
    public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
      IOException, InstantiationException, IllegalAccessException {
      VertexScenarioClasses vertexScenarioClass = (VertexScenarioClasses) generatedMessage;
      Class<?> clazz = Class.forName(vertexScenarioClass.getClassUnderTest());
      this.classUnderTest = castClassToUpperBound(clazz, Computation.class);
      this.vertexIdClass = (Class<I>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getVertexIdClass()), WritableComparable.class);
      this.vertexValueClass = (Class<V>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getVertexValueClass()), Writable.class);
      this.edgeValueClass = (Class<E>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getEdgeValueClass()), Writable.class);
      this.incomingMessageClass = (Class<M1>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getIncomingMessageClass()), Writable.class);
      this.outgoingMessageClass = (Class<M2>) castClassToUpperBound(
        Class.forName(vertexScenarioClass.getOutgoingMessageClass()), Writable.class);
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(super.toString());
      stringBuilder.append("\nclassUnderTest: " + getClassUnderTest().getCanonicalName());
      stringBuilder.append("\nvertexValueClass: " + getVertexValueClass().getCanonicalName());
      stringBuilder.append("\nincomingMessageClass: " + getIncomingMessageClass().getCanonicalName());
      stringBuilder.append("\noutgoingMessageClass: " + getOutgoingMessageClass().getCanonicalName());
      return stringBuilder.toString();
    }

  }

  @SuppressWarnings("unchecked")
  public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    GiraphVertexScenario giraphScenario = (GiraphVertexScenario) generatedMessage;
    this.vertexScenarioClassesWrapper = new VertexScenarioClassesWrapper();
    this.vertexScenarioClassesWrapper.loadFromProto(giraphScenario.getVertexScenarioClasses());

    this.contextWrapper = new VertexContextWrapper();
    this.contextWrapper.loadFromProto(giraphScenario.getContext());
    
    if (giraphScenario.hasException()) {
      this.exceptionWrapper = new ExceptionWrapper();
      this.exceptionWrapper.loadFromProto(giraphScenario.getException());
    }
  }

  @Override
  public  GeneratedMessage buildProtoObject() {
    GiraphVertexScenario.Builder giraphScenarioBuilder = GiraphVertexScenario.newBuilder();
    giraphScenarioBuilder.setVertexScenarioClasses(
      (VertexScenarioClasses) vertexScenarioClassesWrapper.buildProtoObject());
    giraphScenarioBuilder.setContext((VertexContext) contextWrapper.buildProtoObject());
    if (hasExceptionWrapper()) {
      giraphScenarioBuilder.setException((Exception) exceptionWrapper.buildProtoObject());
    }
    GiraphVertexScenario giraphScenario = giraphScenarioBuilder.build();
    return giraphScenario;
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return GiraphVertexScenario.parseFrom(inputStream);
  }

  public VertexScenarioClassesWrapper getVertexScenarioClassesWrapper() {
    return vertexScenarioClassesWrapper;
  }

  public void setVertexScenarioClassesWrapper(VertexScenarioClassesWrapper vertexScenarioClassesWrapper) {
    this.vertexScenarioClassesWrapper = vertexScenarioClassesWrapper;
  }
}
