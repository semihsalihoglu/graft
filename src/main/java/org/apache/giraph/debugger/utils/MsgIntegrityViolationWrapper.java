package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.giraph.debugger.Integrity.MessageIntegrityViolation;
import org.apache.giraph.debugger.Integrity.MessageIntegrityViolation.ExtendedOutgoingMessage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.protobuf.GeneratedMessage;

/**
 * A wrapper class around the contents of MessageIntegrityViolation inside
 * integrity.proto. In scenario.proto most things are stored as serialized byte
 * arrays and this class gives them access through the java classes that those
 * byte arrays serialize.
 * 
 * @param <I>
 *          vertex ID class.
 * @param <M2>
 *          outgoing message class.
 * @author Semih Salihoglu
 */
@SuppressWarnings("rawtypes")
public class MsgIntegrityViolationWrapper<I extends WritableComparable, M2 extends Writable>
  extends BaseScenarioAndIntegrityWrapper<I> {

  private Class<M2> outgoingMessageClass;
  private List<ExtendedOutgoingMessageWrapper> extendedOutgoingMessageWrappers =
    new ArrayList<>();
  private long superstepNo;

  // Empty constructor to be used for loading from HDFS.
  public MsgIntegrityViolationWrapper() {
  }

  public MsgIntegrityViolationWrapper(Class<I> vertexIdClass,
    Class<M2> outgoingMessageClass) {
    initialize(vertexIdClass, outgoingMessageClass);
  }

  private void initialize(Class<I> vertexIdClass, Class<M2> outgoingMessageClass) {
    super.initialize(vertexIdClass);
    this.outgoingMessageClass = outgoingMessageClass;
  }

  public Collection<ExtendedOutgoingMessageWrapper> getExtendedOutgoingMessageWrappers() {
    return extendedOutgoingMessageWrappers;
  }

  public void addMsgWrapper(I srcId, I destinationId, M2 message) {
    extendedOutgoingMessageWrappers.add(new ExtendedOutgoingMessageWrapper(
      DebuggerUtils.makeCloneOf(srcId, vertexIdClass), DebuggerUtils
        .makeCloneOf(destinationId, vertexIdClass), DebuggerUtils.makeCloneOf(
        message, outgoingMessageClass)));
  }

  public int numMsgWrappers() {
    return extendedOutgoingMessageWrappers.size();
  }

  public Class<M2> getOutgoingMessageClass() {
    return outgoingMessageClass;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(super.toString());
    stringBuilder.append("\noutgoingMessageClass: " +
      getOutgoingMessageClass().getCanonicalName());
    for (ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper : getExtendedOutgoingMessageWrappers()) {
      stringBuilder.append("\n" + extendedOutgoingMessageWrapper);
    }
    return stringBuilder.toString();
  }

  public class ExtendedOutgoingMessageWrapper extends BaseWrapper {
    public I srcId;
    public I destinationId;
    public M2 message;

    public ExtendedOutgoingMessageWrapper(I srcId, I destinationId, M2 message) {
      this.srcId = srcId;
      this.destinationId = destinationId;
      this.message = message;
    }

    public ExtendedOutgoingMessageWrapper() {
    }

    @Override
    public String toString() {
      return "extendedOutgoingMessage: srcId: " + srcId + " destinationId: " +
        destinationId + " message: " + message;
    }

    @Override
    public GeneratedMessage buildProtoObject() {
      ExtendedOutgoingMessage.Builder extendedOutgoingMessageBuilder =
        ExtendedOutgoingMessage.newBuilder();
      extendedOutgoingMessageBuilder.setSrcId(toByteString(srcId));
      extendedOutgoingMessageBuilder
        .setDestinationId(toByteString(destinationId));
      extendedOutgoingMessageBuilder.setMsgData(toByteString(message));
      return extendedOutgoingMessageBuilder.build();
    }

    @Override
    public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
        throws IOException {
      return ExtendedOutgoingMessage.parseFrom(inputStream);
    }

    @Override
    public void loadFromProto(GeneratedMessage generatedMessage)
        throws ClassNotFoundException, IOException, InstantiationException,
      IllegalAccessException {
      ExtendedOutgoingMessage extendedOutgoingMessage =
        (ExtendedOutgoingMessage) generatedMessage;
      this.srcId = DebuggerUtils.newInstance(vertexIdClass);
      fromByteString(extendedOutgoingMessage.getSrcId(), this.srcId);
      this.destinationId = DebuggerUtils.newInstance(vertexIdClass);
      fromByteString(extendedOutgoingMessage.getDestinationId(),
        this.destinationId);
      this.message = DebuggerUtils.newInstance(outgoingMessageClass);
      fromByteString(extendedOutgoingMessage.getMsgData(), this.message);
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
    messageIntegrityViolationBuilder.setVertexIdClass(getVertexIdClass()
      .getName());
    messageIntegrityViolationBuilder
      .setOutgoingMessageClass(getOutgoingMessageClass().getName());
    messageIntegrityViolationBuilder.setSuperstepNo(getSuperstepNo());
    for (ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper : extendedOutgoingMessageWrappers) {
      messageIntegrityViolationBuilder
        .addMessage((ExtendedOutgoingMessage) extendedOutgoingMessageWrapper
          .buildProtoObject());
    }
    return messageIntegrityViolationBuilder.build();
  }

  @SuppressWarnings("unchecked")
  public void loadFromProto(GeneratedMessage generatedMessage)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    MessageIntegrityViolation msgIntegrityViolation =
      (MessageIntegrityViolation) generatedMessage;
    Class<I> vertexIdClass =
      (Class<I>) castClassToUpperBound(Class.forName(msgIntegrityViolation
        .getVertexIdClass()), WritableComparable.class);

    Class<M2> outgoingMessageClass =
      (Class<M2>) castClassToUpperBound(Class.forName(msgIntegrityViolation
        .getOutgoingMessageClass()), Writable.class);

    initialize(vertexIdClass, outgoingMessageClass);
    setSuperstepNo(msgIntegrityViolation.getSuperstepNo());

    for (ExtendedOutgoingMessage extendOutgoingMessage : msgIntegrityViolation
      .getMessageList()) {
      ExtendedOutgoingMessageWrapper extendedOutgoingMessageWrapper =
        new ExtendedOutgoingMessageWrapper();
      extendedOutgoingMessageWrapper.loadFromProto(extendOutgoingMessage);
      extendedOutgoingMessageWrappers.add(extendedOutgoingMessageWrapper);
    }
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return MessageIntegrityViolation.parseFrom(inputStream);
  }
}
