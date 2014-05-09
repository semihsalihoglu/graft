package stanford.infolab.plugin.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.VelocityException;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import stanford.infolab.debugger.utils.GiraphScenarioWrapper;

/**
 * This is a code generator which can generate the JUnit test cases for a Giraph.
 * 
 * @author Brian Truong Ba Quan
 */
public class TestCaseGenerator {
  
  public TestCaseGenerator() {
    Velocity.setProperty(VelocityEngine.RESOURCE_LOADER, "class");
    Velocity.setProperty("class." + VelocityEngine.RESOURCE_LOADER + ".class", 
        ClasspathResourceLoader.class.getName());
    Velocity.init();
  }

  public <I extends WritableComparable<I>, V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable> String generateTest(
      GiraphScenarioWrapper<I, V, E, M1, M2> input, String testPackage) throws VelocityException,
      IOException {
    VelocityContext context = new VelocityContext();

    context.put("package", testPackage);

    @SuppressWarnings("rawtypes")
    HashSet<Class> usedTypes = new LinkedHashSet<>(6);
    usedTypes.add(input.getClassUnderTest());
    usedTypes.add(input.getVertexIdClass());
    usedTypes.add(input.getVertexValueClass());
    usedTypes.add(input.getEdgeValueClass());
    usedTypes.add(input.getIncomingMessageClass());
    usedTypes.add(input.getOutgoingMessageClass());
    context.put("usedTypes", usedTypes);

    context.put("classUnderTestName", new String(input.getClassUnderTest().getSimpleName()));

    context.put("vertexIdType", input.getVertexIdClass().getSimpleName());
    context.put("vertexValueType", input.getVertexValueClass().getSimpleName());
    context.put("edgeValueType", input.getEdgeValueClass().getSimpleName());
    context.put("inMsgType", input.getIncomingMessageClass().getSimpleName());
    context.put("outMsgType", input.getOutgoingMessageClass().getSimpleName());

    context.put("vertexId", input.getContextWrapper().getVertexIdWrapper());
    context.put("vertexValue", input.getContextWrapper().getVertexValueBeforeWrapper());
    context.put("inMsgs", input.getContextWrapper().getIncomingMessageWrappers());
    context.put("neighbors", input.getContextWrapper().getNeighborWrappers());

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("TestCaseTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  @SuppressWarnings("rawtypes")
  public String generateClassUnderTestField(GiraphScenarioWrapper scenario) {
    return "private " + scenario.getClassUnderTest().getSimpleName() + " classUnderTest;";
  }
  
  @SuppressWarnings("rawtypes")
  public String generateConfField(GiraphScenarioWrapper scenario) {
    return String.format("private ImmutableClassesGiraphConfiguration<%s, %s, %s> conf;", 
        scenario.getVertexIdClass().getSimpleName(), scenario.getVertexValueClass().getSimpleName(),
        scenario.getEdgeValueClass().getSimpleName());
  }
  
  @SuppressWarnings("rawtypes")
  public String generateMockEnvField(GiraphScenarioWrapper scenario) {
    return String.format("private MockedEnvironment<%s, %s, %s, %s> mockEnv;",
        scenario.getVertexIdClass().getSimpleName(), scenario.getVertexValueClass().getSimpleName(),
        scenario.getEdgeValueClass().getSimpleName(), scenario.getOutgoingMessageClass().getSimpleName());
  }

  @SuppressWarnings("rawtypes")
  public String generateSetUp(GiraphScenarioWrapper input) throws VelocityException, IOException {
    VelocityContext context = new VelocityContext();

    context.put("classUnderTestName", new String(input.getClassUnderTest().getSimpleName()));

    context.put("vertexIdType", input.getVertexIdClass().getSimpleName());
    context.put("vertexValueType", input.getVertexValueClass().getSimpleName());
    context.put("edgeValueType", input.getEdgeValueClass().getSimpleName());
    context.put("inMsgType", input.getIncomingMessageClass().getSimpleName());
    context.put("outMsgType", input.getOutgoingMessageClass().getSimpleName());

    context.put("vertexId", input.getContextWrapper().getVertexIdWrapper());
    context.put("vertexValue", input.getContextWrapper().getVertexValueBeforeWrapper());
    context.put("inMsgs", input.getContextWrapper().getIncomingMessageWrappers());
    context.put("neighbors", input.getContextWrapper().getNeighborWrappers());

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("SetUpTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  @SuppressWarnings("rawtypes")
  public String generateTestCompute(GiraphScenarioWrapper input) throws VelocityException, IOException {
    VelocityContext context = new VelocityContext();
    
    context.put("classUnderTestName", new String(input.getClassUnderTest().getSimpleName()));
    
    context.put("vertexIdType", input.getVertexIdClass().getSimpleName());
    context.put("vertexValueType", input.getVertexValueClass().getSimpleName());
    context.put("edgeValueType", input.getEdgeValueClass().getSimpleName());
    context.put("inMsgType", input.getIncomingMessageClass().getSimpleName());
    context.put("outMsgType", input.getOutgoingMessageClass().getSimpleName());
    
    context.put("vertexId", input.getContextWrapper().getVertexIdWrapper());
    context.put("vertexValue", input.getContextWrapper().getVertexValueBeforeWrapper());
    context.put("inMsgs", input.getContextWrapper().getIncomingMessageWrappers());
    context.put("neighbors", input.getContextWrapper().getNeighborWrappers());
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("TestComputeTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
}
