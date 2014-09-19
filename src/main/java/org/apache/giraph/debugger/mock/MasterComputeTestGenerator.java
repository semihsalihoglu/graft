package org.apache.giraph.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.master.MasterCompute;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.VelocityException;

/**
 * A code generator to generate test cases to test {@link MasterCompute}
 * 
 * @author Brian Truong Ba Quan
 */
public class MasterComputeTestGenerator extends TestGenerator {

  public MasterComputeTestGenerator() {
    super();
  }

  public String generateTest(GiraphMasterScenarioWrapper input,
    String testPackage) throws VelocityException, IOException,
    ClassNotFoundException {
    return generateTest(input, testPackage, null);
  }

  public String generateTest(GiraphMasterScenarioWrapper input,
    String testPackage, String className) throws VelocityException,
    IOException, ClassNotFoundException {
    VelocityContext context = buildContext(input, testPackage, className);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("MasterComputeTestTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  private VelocityContext buildContext(
    GiraphMasterScenarioWrapper giraphScenarioWrapper, String testPackage,
    String className) throws ClassNotFoundException {
    ContextBuilder builder = new ContextBuilder();

    Class<?> classUnderTest = Class.forName(giraphScenarioWrapper
      .getMasterClassUnderTest());
    builder.addTestClassInfo(testPackage, classUnderTest, className);
    builder.addCommonMasterVertexContext(giraphScenarioWrapper
      .getCommonVertexMasterContextWrapper());

    return builder.getContext();
  }

}
