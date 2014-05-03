package stanford.infolab.debugger.utils;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.graph.Computation;
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
  
  class ContextWrapper {
    private I vertexId;
    private V vertexValue;
    private ArrayList<M1> inMsgs;
    private ArrayList<NeighborWrapper> neighbors;
    private ArrayList<OutgoingMessageWrapper> outMsgs;

    public ContextWrapper() {
      reset();
    }

    void reset() {
      this.vertexId = null;
      this.vertexValue = null;
      this.inMsgs = new ArrayList<M1>();
      this.neighbors = new ArrayList<NeighborWrapper>();
      this.outMsgs = new ArrayList<OutgoingMessageWrapper>();
    }

    public I getVertexIdWrapper() {
      return vertexId;
    }

    public void setVertexIdWrapper(I vertexId) {
      this.vertexId = vertexId;
    }

    public V getVertexValueWrapper() {
      return vertexValue;
    }

    public void setVertexValueWrapper(V vertexValue) {
      this.vertexValue = vertexValue;
    }

    public void addIncomingMessageWrapper(M1 message) {
      inMsgs.add(message);
    }

    public Collection<M1> getIncomingMessageWrappers() {
      return inMsgs;
    }

    public void addOutgoingMessageWrapper(I receiverId, M2 message) {
      outMsgs.add(new OutgoingMessageWrapper(receiverId, message));
    }

    public Collection<OutgoingMessageWrapper> getOutgoingMessageWrappers() {
      return outMsgs;
    }

    public void addNeighborWrapper(I neighborId, E edgeValue) {
      neighbors.add(new NeighborWrapper(neighborId, edgeValue));
    }

    public Collection<NeighborWrapper> getNeighborWrappers() {
      return neighbors;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("vertexId: " + getVertexIdWrapper());
      stringBuilder.append("\nvertexValue: " + getVertexValueWrapper());
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

      public I nbrId;
      public E edgeValue;

      public NeighborWrapper(I nbrId, E edgeValue) {
        this.nbrId = nbrId;
        this.edgeValue = edgeValue;
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
