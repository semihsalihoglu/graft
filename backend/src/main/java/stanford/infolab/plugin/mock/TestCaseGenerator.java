package stanford.infolab.plugin.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
  
  @SuppressWarnings("rawtypes")
  private Set<Class> unsolvedWritableSet = new HashSet<>();
  
  public TestCaseGenerator() {
    Velocity.setProperty(VelocityEngine.RESOURCE_LOADER, "class");
    Velocity.setProperty("class." + VelocityEngine.RESOURCE_LOADER + ".class", 
        ClasspathResourceLoader.class.getName());
    Velocity.init();
  }
  
  @SuppressWarnings("rawtypes")
  public Set<Class> getUnsolvedWritableSet() {
    return unsolvedWritableSet;
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
    context.put("helper", new FormatHelper());

    context.put("classUnderTestName", new String(input.getClassUnderTest().getSimpleName()));

    context.put("vertexIdType", input.getVertexIdClass().getSimpleName());
    context.put("vertexValueType", input.getVertexValueClass().getSimpleName());
    context.put("edgeValueType", input.getEdgeValueClass().getSimpleName());
    context.put("inMsgType", input.getIncomingMessageClass().getSimpleName());
    context.put("outMsgType", input.getOutgoingMessageClass().getSimpleName());
    
    context.put("superstepNo", input.getContextWrapper().getSuperstepNoWrapper());
    //TODO(brian): Use default values for now, change when data are available
    context.put("nVertices", 1);
    context.put("nEdges", 1);
    
    //TODO(brian): Use default values for now, change when data are available
    List<Aggregator> aggregators = Arrays.asList(new Aggregator("testAggr", 
        new DoubleWritable(1.0)));
    context.put("aggregators", aggregators);
    
    //TODO(brian): Use default values for now, change when data are available
    List<Config> configs = Arrays.asList(new Config("Epsilon", 0.01f), new Config("MaxSteps", 100), 
        new Config("MaxError", 655.36f));
    context.put("configs", configs);
    
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
  
  @SuppressWarnings({"rawtypes"})
  public String generateTestCompute(GiraphScenarioWrapper input) throws VelocityException, IOException {
    unsolvedWritableSet.clear();
    
    VelocityContext context = new VelocityContext();
    context.put("helper", new FormatHelper());
    
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
  
  public String generateReadWritableFromString(String className) throws VelocityException, IOException {
    VelocityContext context = new VelocityContext();
    context.put("class", className);
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ReadWritableFromStringTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  public class FormatHelper {
    
    private DecimalFormat decimalFormat = new DecimalFormat("#.#####");
    
    public String formatWritable(Writable writable) {
      if (writable instanceof NullWritable) {
        return "NullWritable.get()";
      } else if (writable instanceof BooleanWritable) {
        return String.format("new BooleanWritable(%s)", format(((BooleanWritable)writable).get()));
      } else if (writable instanceof ByteWritable) {
        return String.format("new ByteWritable(%s)", format(((ByteWritable)writable).get()));
      } else if (writable instanceof IntWritable) {
        return String.format("new IntWritable(%s)", format(((IntWritable)writable).get()));
      } else if (writable instanceof LongWritable) {
        return String.format("new LongWritable(%s)", format(((LongWritable)writable).get()));
      } else if (writable instanceof FloatWritable) {
        return String.format("new FloatWritable(%s)", format(((FloatWritable)writable).get()));
      } else if (writable instanceof DoubleWritable) {
        return String.format("new DoubleWritable(%s)", format(((DoubleWritable)writable).get()));
      } else if (writable instanceof Text) {
        return String.format("new Text(%s)", ((Text)writable).toString());
      } else {
        unsolvedWritableSet.add(writable.getClass());
        String str = new String(WritableUtils.writeToByteArray(writable));
        return String.format("(%s)read%sFromString(\"%s\")", writable.getClass().getSimpleName(),
            writable.getClass().getSimpleName(), str);
      }
    }
    
    public String format(Object input) {
      if (input instanceof Boolean || input instanceof Byte || input instanceof Character ||
          input instanceof Short || input instanceof Integer) {
        return input.toString();
      } else if (input instanceof Long) {
        return input.toString() + "l";
      } else if (input instanceof Float) {
        return decimalFormat.format(input) + "f";
      } else if (input instanceof Double) {
        double val = ((Double)input).doubleValue();
        if (val == Double.MAX_VALUE)
          return "Double.MAX_VALUE";
        else if (val == Double.MIN_VALUE)
          return "Double.MIN_VALUE";
        else {
          BigDecimal bd = new BigDecimal(val);
          return bd.toEngineeringString() + "d";
        }
      } else {
        return input.toString();
      }
    }
  }
  
  public static class Aggregator {
    private String key;
    private Writable value;
    
    public Aggregator(String key, Writable value) {
      this.key = key;
      this.value = value;
    }
    
    public String getKey() {
      return key;
    }
    
    public Writable getValue() {
      return value;
    }
    
    public String getClassName() {
      return key.getClass().getName();
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
}
