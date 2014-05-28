package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.utils.WritableUtils;

import org.apache.giraph.debugger.GiraphAggregator.AggregatedValue;
import org.apache.giraph.debugger.Scenario.Exception;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around {@link org.apache.giraph.debugger.Scenario.Exception}
 * protocol buffer.
 * 
 * @author semihsalihoglu
 */
public class ExceptionWrapper extends BaseWrapper {
  private String errorMessage = "";
  private String stackTrace = "";

  public ExceptionWrapper() {}

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
    // We append with "" to guard against null pointer exceptions
    return "" + errorMessage;
  }

  public String getStackTrace() {
    // We append with "" to guard against null pointer exceptions
    return "" + stackTrace;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    Exception.Builder exceptionBuilder = Exception.newBuilder();
    exceptionBuilder.setMessage(getErrorMessage());
    exceptionBuilder.setStackTrace(getStackTrace());
    return exceptionBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return Exception.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage generatedMessage) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    Exception exceptionProto = (Exception) generatedMessage;
    this.errorMessage = exceptionProto.getMessage();
    this.stackTrace = exceptionProto.getStackTrace();
  }

  public void setErrorMessage(String errorMessage) {
    // We append "" to guard against null pointer exceptions
    this.errorMessage = "" + errorMessage;
  }

  public void setStackTrace(String stackTrace) {
    // We append "" to guard against null pointer exceptions
    this.stackTrace = "" + stackTrace;
  }
}