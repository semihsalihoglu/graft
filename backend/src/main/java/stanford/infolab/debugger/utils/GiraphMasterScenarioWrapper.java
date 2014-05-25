package stanford.infolab.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import stanford.infolab.debugger.Scenario.CommonVertexMasterContext;
import stanford.infolab.debugger.Scenario.Exception;
import stanford.infolab.debugger.Scenario.GiraphMasterScenario;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around {@link stanford.infolab.debugger.Scenario.GiraphMasterScenario}
 * protocol buffer.
 * 
 * @author semihsalihoglu
 */
public class GiraphMasterScenarioWrapper extends BaseWrapper {
  private String masterClassUnderTest;
  private CommonVertexMasterContextWrapper commonVertexMasterContextWrapper = null;
  private ExceptionWrapper exceptionWrapper = null;
  
  public GiraphMasterScenarioWrapper() {}
  
  public GiraphMasterScenarioWrapper(String masterClassUnderTest) {
    this.masterClassUnderTest = masterClassUnderTest;
    this.commonVertexMasterContextWrapper = new CommonVertexMasterContextWrapper();
    this.exceptionWrapper = null;
  }

  public String getMasterClassUnderTest() {
    return masterClassUnderTest;
  }

  public CommonVertexMasterContextWrapper getCommonVertexMasterContextWrapper() {
    return commonVertexMasterContextWrapper;
  }

  public void setCommonVertexMasterContextWrapper(CommonVertexMasterContextWrapper commonVertexMasterContextWrapper) {
    this.commonVertexMasterContextWrapper = commonVertexMasterContextWrapper;
  }

  public ExceptionWrapper getExceptionWrapper() {
    return exceptionWrapper;
  }

  public void setExceptionWrapper(ExceptionWrapper exceptionWrapper) {
    this.exceptionWrapper = exceptionWrapper;
  }

  public boolean hasExceptionWrapper() {
    return exceptionWrapper != null;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    GiraphMasterScenario.Builder giraphMasterScenarioBuilder = GiraphMasterScenario.newBuilder();
    giraphMasterScenarioBuilder.setMasterClassUnderTest(masterClassUnderTest);
    giraphMasterScenarioBuilder.setCommonContext(
      (CommonVertexMasterContext) commonVertexMasterContextWrapper.buildProtoObject());
    if (hasExceptionWrapper()) {
      giraphMasterScenarioBuilder.setException((Exception) exceptionWrapper.buildProtoObject());
    }
    return giraphMasterScenarioBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream) throws IOException {
    return GiraphMasterScenario.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage protoObject) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    GiraphMasterScenario giraphMasterScenario = (GiraphMasterScenario) protoObject;
    this.masterClassUnderTest = giraphMasterScenario.getMasterClassUnderTest();
    this.commonVertexMasterContextWrapper = new CommonVertexMasterContextWrapper();
    this.commonVertexMasterContextWrapper.loadFromProto(giraphMasterScenario.getCommonContext());
    if (giraphMasterScenario.hasException()) {
      this.exceptionWrapper = new ExceptionWrapper();
      this.exceptionWrapper.loadFromProto(giraphMasterScenario.getException());
    }
  }
  
  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("masterClassUnderTest: " + masterClassUnderTest);
    stringBuilder.append("\n" + commonVertexMasterContextWrapper.toString());
    stringBuilder.append("\nhasExceptionWrapper: " + hasExceptionWrapper());
    if (hasExceptionWrapper()) {
      stringBuilder.append("\n" + exceptionWrapper.toString());
    }
    return stringBuilder.toString();
  }
}
