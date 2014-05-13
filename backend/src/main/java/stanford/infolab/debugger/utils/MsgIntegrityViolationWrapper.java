package stanford.infolab.debugger.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import stanford.infolab.debugger.Integrity.MessageIntegrityViolation;
import stanford.infolab.debugger.Integrity.MessageIntegrityViolation.ExtendedOutgoingMessage;

import com.google.protobuf.GeneratedMessage;

/**
 * A wrapper class around the contents of MessageIntegrityViolation inside integrity.proto. In
 * scenario.proto most things are stored as serialized byte arrays and this class gives
 * them access through the java classes that those byte arrays serialize.
 * 
 * @param <I> vertex ID class.
 * @param <M2> outgoing message class.
 * @author Semih Salihoglu
 */
@SuppressWarnings("rawtypes")
public class MsgIntegrityViolationWrapper<I extends WritableComparable, M2 extends Writable> 
  extends BaseScenarioAndIntegrityWrapper<I>{

  private Class<M2> outgoingMessageClass;
  private List<ExtendedOutgoingMessageWrapper> msgWrappers = new ArrayList<>();
  private long superstepNo;

  // Empty constructor to be used for loading from HDFS.
  public MsgIntegrityViolationWrapper() {}
  
  public MsgIntegrityViolationWrapper(Class<I> vertexIdClass,Class<M2> outgoingMessageClass) {
    initialize(vertexIdClass, outgoingMessageClass);
  }

  private void initialize(Class<I> vertexIdClass, Class<M2> outgoingMessageClass) {
    super.initialize(vertexIdClass);
    this.outgoingMessageClass = outgoingMessageClass;    
  }

  public Collection<ExtendedOutgoingMessageWrapper> getExtendedOutgoingMessageWrappers() {
    return msgWrappers;
  }

  public void addMsgWrapper(I srcId, I destinationId, M2 message) {
    msgWrappers.add(new ExtendedOutgoingMessageWrapper(makeCloneOf(srcId, vertexIdClass),
      makeCloneOf(destinationId, vertexIdClass), makeCloneOf(message, outgoingMessageClass)));
  }

  public int numMsgWrappers() {
    return msgWrappers.size();
  }
  
  public Class<M2> getOutgoingMessageClass() {
    return outgoingMessageClass;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\noutgoingMessageClass: " + getOutgoingMessageClass().getCanonicalName());
    for (ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper
      : getExtendedOutgoingMessageWrappers()) {
      stringBuilder.append("\n" + extendedOutgoingMessageWrapper);
    }
    return stringBuilder.toString();
  }
  
  public class ExtendedOutgoingMessageWrapper {
    public I srcId;
    public I destinationId;
    public M2 message;

    public ExtendedOutgoingMessageWrapper(I srcId, I destinationId, M2 message) {
      this.srcId = srcId;
      this.destinationId = destinationId;
      this.message = message;
    }
    
    @Override
    public String toString() {
      return "extendedOutgoingMessage: srcId: " + srcId + " destinationId: " + destinationId
        + " message: " + message; 
    }
  }

  public long getSuperstepNo() {
    return superstepNo;
  }

  public void setSuperstepNo(long superstepNo) {
    this.superstepNo = superstepNo;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    MessageIntegrityViolation.Builder messageIntegrityViolationBuilder =
      MessageIntegrityViolation.newBuilder();
    messageIntegrityViolationBuilder.setVertexIdClass(getVertexIdClass().getName());
    messageIntegrityViolationBuilder.setOutgoingMessageClass(getOutgoingMessageClass().getName());
    messageIntegrityViolationBuilder.setSuperstepNo(getSuperstepNo());
    for (ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper : msgWrappers) {
      ExtendedOutgoingMessage.Builder extendedOutgoingMessageBuilder =
        ExtendedOutgoingMessage.newBuilder();
      extendedOutgoingMessageBuilder.setSrcId(
        toByteString(extendedOutgoingMessageWrapper.srcId));
      extendedOutgoingMessageBuilder.setDestinationId(
        toByteString(extendedOutgoingMessageWrapper.destinationId));
      extendedOutgoingMessageBuilder.setMsgData(
        toByteString(extendedOutgoingMessageWrapper.message));
      messageIntegrityViolationBuilder.addMessage(extendedOutgoingMessageBuilder.build());
    }
    return messageIntegrityViolationBuilder.build();
  }

  @SuppressWarnings("unchecked")
  public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
    IOException {
    MessageIntegrityViolation msgIntegrityViolation = (MessageIntegrityViolation) generatedMessage;
    Class<I> vertexIdClass = (Class<I>) castClassToUpperBound(
      Class.forName(msgIntegrityViolation.getVertexIdClass()), WritableComparable.class);

    Class<M2> outgoingMessageClass = (Class<M2>) castClassToUpperBound(
      Class.forName(msgIntegrityViolation.getOutgoingMessageClass()), Writable.class);

    initialize(vertexIdClass, outgoingMessageClass);
    setSuperstepNo(msgIntegrityViolation.getSuperstepNo());

    for (ExtendedOutgoingMessage outmsg : msgIntegrityViolation.getMessageList()) {
      I srcId = newInstance(vertexIdClass);
      fromByteString(outmsg.getSrcId(), srcId);
      I destinationId = newInstance(vertexIdClass);
      fromByteString(outmsg.getDestinationId(), destinationId);
      M2 msg = newInstance(outgoingMessageClass);
      fromByteString(outmsg.getMsgData(), msg);
      addMsgWrapper(srcId, destinationId, msg);
    }
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return MessageIntegrityViolation.parseFrom(inputStream);
  }
}
