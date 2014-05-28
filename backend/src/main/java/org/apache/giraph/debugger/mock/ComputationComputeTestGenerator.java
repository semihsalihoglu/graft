package org.apache.giraph.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;

import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper.OutgoingMessageWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexScenarioClassesWrapper;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.VelocityException;

/**
 * This is a code generator which can generate the JUnit test cases for a Giraph.
 * 
 * @author Brian Truong Ba Quan
 */
public class ComputationComputeTestGenerator extends TestGenerator {
  
  public ComputationComputeTestGenerator() {
    super();
  }
  
  @SuppressWarnings("rawtypes")
  public String generateTest(GiraphVertexScenarioWrapper input, String testPackage) 
      throws VelocityException, IOException {
    return generateTest(input, testPackage, null);
  }

  @SuppressWarnings("rawtypes")
  public String generateTest(GiraphVertexScenarioWrapper input, String testPackage, String className) 
      throws VelocityException, IOException {
    VelocityContext context = buildContext(input, testPackage, className);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ComputeTestTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  @SuppressWarnings("rawtypes")
  public String generateClassUnderTestField(GiraphVertexScenarioWrapper scenario) {
    return "private " +scenario.getVertexScenarioClassesWrapper().getClassUnderTest()
      .getSimpleName() + " classUnderTest;";
  }
  
  @SuppressWarnings("rawtypes")
  public String generateConfField(GiraphVertexScenarioWrapper scenario) {
    return String.format("private ImmutableClassesGiraphConfiguration<%s, %s, %s> conf;", 
        scenario.getVertexScenarioClassesWrapper().getVertexIdClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getVertexValueClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getEdgeValueClass().getSimpleName());
  }
  
  @SuppressWarnings("rawtypes")
  public String generateMockEnvField(GiraphVertexScenarioWrapper scenario) {
    return String.format("private MockedEnvironment<%s, %s, %s, %s> mockEnv;",
        scenario.getVertexScenarioClassesWrapper().getVertexIdClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getVertexValueClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getEdgeValueClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getOutgoingMessageClass().getSimpleName());
  }
  
  @SuppressWarnings("rawtypes")
  public String generateProcessorField(GiraphVertexScenarioWrapper scenario) {
    return String.format("private WorkerClientRequestProcessor<%s, %s, %s> processor;", 
        scenario.getVertexScenarioClassesWrapper().getVertexIdClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getVertexValueClass().getSimpleName(),
        scenario.getVertexScenarioClassesWrapper().getEdgeValueClass().getSimpleName());
  }

  @SuppressWarnings("rawtypes")
  public String generateSetUp(GiraphVertexScenarioWrapper input) throws VelocityException, IOException {
    VelocityContext context = buildContext(input);
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ComputeSetUpFuncTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  @SuppressWarnings({"rawtypes"})
  public String generateTestCompute(GiraphVertexScenarioWrapper input) throws VelocityException, IOException {
    getWritableReadFromString().clear();
    
    VelocityContext context = buildContext(input);
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ComputeTestFuncTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  public String generateReadWritableFromString(String className) throws VelocityException, IOException {
    VelocityContext context = new VelocityContext();
    context.put("class", className);
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ReadWritableFromStringTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  @SuppressWarnings("rawtypes")
  private VelocityContext buildContext(GiraphVertexScenarioWrapper giraphScenarioWrapper) {
    return buildContext(giraphScenarioWrapper, null, null);
  }
  
  @SuppressWarnings("rawtypes")
  private VelocityContext buildContext(GiraphVertexScenarioWrapper giraphScenarioWrapper, 
      String testPackage, String className) {
    ComputeContextBuilder builder = new ComputeContextBuilder();
    VertexScenarioClassesWrapper vertexScenarioClassesWrapper =
        giraphScenarioWrapper.getVertexScenarioClassesWrapper();
    builder.addVertexScenarioClassesWrapper(vertexScenarioClassesWrapper);
    builder.addTestClassInfo(testPackage, vertexScenarioClassesWrapper.getClassUnderTest(), className); 
    builder.addCommonMasterVertexContext(giraphScenarioWrapper.getContextWrapper()
        .getCommonVertexMasterContextWrapper());
    builder.addVertexTypes(vertexScenarioClassesWrapper);
    builder.addVertexData(giraphScenarioWrapper.getContextWrapper());
    return builder.getContext();
  }
  
  protected class ComputeContextBuilder extends ContextBuilder {
   
    @SuppressWarnings("rawtypes")
    public void addVertexTypes(VertexScenarioClassesWrapper vertexScenarioClassesWrapper) {
      context.put("vertexIdType", vertexScenarioClassesWrapper.getVertexIdClass().getSimpleName());
      context.put("vertexValueType", vertexScenarioClassesWrapper.getVertexValueClass().getSimpleName());
      context.put("edgeValueType", vertexScenarioClassesWrapper.getEdgeValueClass().getSimpleName());
      context.put("inMsgType", vertexScenarioClassesWrapper.getIncomingMessageClass().getSimpleName());
      context.put("outMsgType", vertexScenarioClassesWrapper.getOutgoingMessageClass().getSimpleName());
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void addVertexData(VertexContextWrapper vertexContextWrapper) {
      context.put("vertexId", vertexContextWrapper.getVertexIdWrapper());
      context.put("vertexValue", vertexContextWrapper.getVertexValueBeforeWrapper());
      context.put("vertexValueAfter", vertexContextWrapper.getVertexValueAfterWrapper());
      context.put("inMsgs", vertexContextWrapper.getIncomingMessageWrappers());
      context.put("neighbors", vertexContextWrapper.getNeighborWrappers());

      HashMap<OutgoingMessageWrapper, OutMsg> outMsgMap = new HashMap<>();
      for (OutgoingMessageWrapper msg : 
          (Collection<OutgoingMessageWrapper>)vertexContextWrapper.getOutgoingMessageWrappers()) {
        if (outMsgMap.containsKey(msg))
          outMsgMap.get(msg).incrementTimes();
        else
          outMsgMap.put(msg, new OutMsg(msg));
      }
      context.put("outMsgs", outMsgMap.values());
    }
  }
  
  public static class Config {
    private String key;
    private Object value;
    
    public Config(String key, Object value) {
      this.key = key;
      this.value = value;
    }
    
    public String getKey() {
      return key;
    }
    
    public Object getValue() {
      if (value instanceof String)
        return "\"" + value + '"';
      else
        return value;
    }
    
    public String getClassStr() {
      // TODO(brian):additional cases can be added up to the input
      if (value instanceof Integer)
        return "Int";
      else if (value instanceof Long)
        return "Long";
      else if (value instanceof Float)
        return "Float";
      else if (value instanceof Boolean)
        return "Boolean";
      else 
        return "";
    }
  }
  
  @SuppressWarnings("rawtypes")
  public static class OutMsg {
    private OutgoingMessageWrapper msg;
    private int times;
    
    public OutMsg(OutgoingMessageWrapper msg) {
      this.msg = msg;
      this.times = 1;
    }
    
    public OutgoingMessageWrapper getMsg() {
      return msg;
    }
    
    public int getTimes() {
      return times;
    }
    
    public void incrementTimes() {
      this.times++;
    }
  }
}
