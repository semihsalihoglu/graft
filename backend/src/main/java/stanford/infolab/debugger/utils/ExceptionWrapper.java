package stanford.infolab.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.utils.WritableUtils;

import stanford.infolab.debugger.GiraphAggregator.AggregatedValue;
import stanford.infolab.debugger.Scenario.Exception;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around {@link stanford.infolab.debugger.Scenario.Exception}
 * protocol buffer.
 * 
 * @author semihsalihoglu
 */
public class ExceptionWrapper extends BaseWrapper {
  private String errorMessage = "";
  private String stackTrace = "";

  public ExceptionWrapper() {}

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

  @Override
  public GeneratedMessage buildProtoObject() {
    Exception.Builder exceptionBuilder = Exception.newBuilder();
    exceptionBuilder.setMessage(errorMessage);
    exceptionBuilder.setStackTrace(stackTrace);
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
    this.errorMessage = errorMessage;
  }

  public void setStackTrace(String stackTrace) {
    this.stackTrace = stackTrace;
  }
}