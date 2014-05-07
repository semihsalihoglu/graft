package stanford.infolab.debugger.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.graph.Computation;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A wrapper class around the contents of scenario.proto. In scenario.proto most things are
 * stored as serialized byte arrays and this class gives them access through the java classes
 * that those byte arrays serialize.
 * 
 * @param <I> vertex ID class.
 * @param <V> vertex value class.
 * @param <E> edge value class.
 * @param <M1> incoming message class.
 * @param <M2> outgoing message class.
 * @author Brian Truong
 */
@SuppressWarnings("rawtypes")
public class GiraphScenarioWrapper<I extends WritableComparable, V extends Writable,
  E extends Writable, M1 extends Writable, M2 extends Writable> {

  private Class<? extends Computation<I, V, E, M1, M2>> classUnderTest;
  private Class<I> vertexIdClass;
  private Class<V> vertexValueClass;
  private Class<E> edgeValueClass;
  private Class<M1> incomingMessageClass;
  private Class<M2> outgoingMessageClass;

  private ContextWrapper contextWrapper = null;

  public GiraphScenarioWrapper(Class<? extends Computation<I, V, E, M1, M2>> classUnderTest,
    Class<I> vertexIdClass, Class<V> vertexValueClass, Class<E> edgeValueClass,
    Class<M1> incomingMessageClass, Class<M2> outgoingMessageClass) {
    this.classUnderTest = classUnderTest;
    this.vertexIdClass = vertexIdClass;
    this.vertexValueClass = vertexValueClass;
    this.edgeValueClass = edgeValueClass;
    this.incomingMessageClass = incomingMessageClass;
    this.outgoingMessageClass = outgoingMessageClass;
  }

  public Class<? extends Computation<I, V, E, M1, M2>> getClassUnderTest() {
    return classUnderTest;
  }

  public Class<I> getVertexIdClass() {
    return vertexIdClass;
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

  public void setContextWrapper(ContextWrapper context) {
    this.contextWrapper = context;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("classUnderTest: " + getClassUnderTest().getCanonicalName());
    stringBuilder.append("\nvertexIdClass: " + getVertexIdClass().getCanonicalName());
    stringBuilder.append("\nvertexValueClass: " + getVertexValueClass().getCanonicalName());
    stringBuilder.append("\nincomingMessageClass: " + getIncomingMessageClass().getCanonicalName());
    stringBuilder.append("\noutgoingMessageClass: " + getOutgoingMessageClass().getCanonicalName());
    stringBuilder.append("\n" + contextWrapper.toString());
    return stringBuilder.toString();
  }
  
  public class ContextWrapper {
    private long superstepNo;
    private I vertexId;
    private V vertexValueBefore;
    private V vertexValueAfter;
    private ArrayList<M1> inMsgs;
    private ArrayList<NeighborWrapper> neighbors;
    private ArrayList<OutgoingMessageWrapper> outMsgs;

    public ContextWrapper() {
      reset();
    }

    public void reset() {
      this.superstepNo = -1;
      this.vertexId = null;
      this.vertexValueBefore = null;
      this.vertexValueAfter = null;
      this.inMsgs = new ArrayList<M1>();
      this.neighbors = new ArrayList<NeighborWrapper>();
      this.outMsgs = new ArrayList<OutgoingMessageWrapper>();
    }

    public long getSuperstepNoWrapper() {
      return superstepNo;
    }

    public void setSuperstepNoWrapper(long superstepNo) {
      this.superstepNo = superstepNo;
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
      // the value passed to setVertexValueAfterWrapper, because it was called later).
      this.vertexValueBefore = makeCloneOf(vertexValueBefore, vertexValueClass);
    }

    public void setVertexValueAfterWrapper(V vertexValueAfter) {
      // See the explanation for making a clone inside setVertexValueBeforeWrapper
      this.vertexValueAfter = makeCloneOf(vertexValueAfter, vertexValueClass);
    }

    public void addIncomingMessageWrapper(M1 message) {
      // See the explanation for making a clone inside setVertexValueBeforeWrapper
      inMsgs.add(makeCloneOf(message, incomingMessageClass));
    }

    public Collection<M1> getIncomingMessageWrappers() {
      return inMsgs;
    }

    public void addOutgoingMessageWrapper(I receiverId, M2 message) {
      // See the explanation for making a clone inside setVertexValueBeforeWrapper
      outMsgs.add(new OutgoingMessageWrapper(makeCloneOf(receiverId, vertexIdClass),
        makeCloneOf(message, outgoingMessageClass)));
    }

    public Collection<OutgoingMessageWrapper> getOutgoingMessageWrappers() {
      return outMsgs;
    }

    public void addNeighborWrapper(I neighborId, E edgeValue) {
      // See the explanation for making a clone inside setVertexValueBeforeWrapper
      neighbors.add(new NeighborWrapper(
        makeCloneOf(neighborId, vertexIdClass), makeCloneOf(edgeValue, edgeValueClass)));
    }

    public Collection<NeighborWrapper> getNeighborWrappers() {
      return neighbors;
    }

    private <T extends Writable> T makeCloneOf(T actualId, Class<T> clazz) {
      T idCopy = GiraphScenearioSaverLoader.newInstance(clazz);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      try {
        actualId.write(dataOutputStream);
      } catch (IOException e) {
        // Throwing a runtime exception because the methods that call other methods
        // such as addNeighborWrapper or addOutgoingMessageWrapper, implement abstract classes
        // or interfaces of Giraph that we can't edit to include a throws statement.
        throw new RuntimeException(e);
      }
      WritableUtils.readFieldsFromByteArray(byteArrayOutputStream.toByteArray(), idCopy);
      byteArrayOutputStream.reset();
      return idCopy;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("superstepNo: " + getSuperstepNoWrapper());
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

      for (OutgoingMessageWrapper outgoingMessage : getOutgoingMessageWrappers()) {
        stringBuilder.append("\n" + outgoingMessage);
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

      public OutgoingMessageWrapper(I receiverId, M2 message) {
        this.destinationId = receiverId;
        this.message = message;
      }
      
      @Override
      public String toString() {
        return "outgoingMessage: destinationId: " + destinationId + " message: " + message; 
      }
    }
  }
}
