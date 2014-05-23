package stanford.infolab.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
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
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.VelocityException;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import stanford.infolab.debugger.utils.CommonVertexMasterContextWrapper;
import stanford.infolab.debugger.utils.GiraphVertexScenarioWrapper;
import stanford.infolab.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper.OutgoingMessageWrapper;
import stanford.infolab.debugger.utils.GiraphVertexScenarioWrapper.VertexScenarioClassesWrapper;

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

  @SuppressWarnings("rawtypes")
  public String generateTest(GiraphVertexScenarioWrapper input, String testPackage) 
      throws VelocityException, IOException {
    VelocityContext context = buildContext(input);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("TestCaseTemplate.vm");
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
      Template template = Velocity.getTemplate("SetUpTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  @SuppressWarnings({"rawtypes"})
  public String generateTestCompute(GiraphVertexScenarioWrapper input) throws VelocityException, IOException {
    unsolvedWritableSet.clear();
    
    VelocityContext context = buildContext(input);
    
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
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  private VelocityContext buildContext(GiraphVertexScenarioWrapper giraphScenarioWrapper) {
    VelocityContext context = new VelocityContext();

    VertexScenarioClassesWrapper vertexScenarioClassesWrapper =
      giraphScenarioWrapper.getVertexScenarioClassesWrapper();
    HashSet<Class> usedTypes = new LinkedHashSet<>(6);
    usedTypes.add(vertexScenarioClassesWrapper.getClassUnderTest());
    usedTypes.add(vertexScenarioClassesWrapper.getVertexIdClass());
    usedTypes.add(vertexScenarioClassesWrapper.getVertexValueClass());
    usedTypes.add(vertexScenarioClassesWrapper.getEdgeValueClass());
    usedTypes.add(vertexScenarioClassesWrapper.getIncomingMessageClass());
    usedTypes.add(vertexScenarioClassesWrapper.getOutgoingMessageClass());
    context.put("usedTypes", usedTypes);

    context.put("helper", new FormatHelper());
    
    context.put("classUnderTestName", new String(
      vertexScenarioClassesWrapper.getClassUnderTest().getSimpleName()));

    context.put("vertexIdType", vertexScenarioClassesWrapper.getVertexIdClass().getSimpleName());
    context.put("vertexValueType", vertexScenarioClassesWrapper.getVertexValueClass().getSimpleName());
    context.put("edgeValueType", vertexScenarioClassesWrapper.getEdgeValueClass().getSimpleName());
    context.put("inMsgType", vertexScenarioClassesWrapper.getIncomingMessageClass().getSimpleName());
    context.put("outMsgType", vertexScenarioClassesWrapper.getOutgoingMessageClass().getSimpleName());
    
    CommonVertexMasterContextWrapper commonVertexMasterContextWrapper =
      giraphScenarioWrapper.getContextWrapper().getCommonVertexMasterContextWrapper();
    context.put("superstepNo", commonVertexMasterContextWrapper.getSuperstepNoWrapper());
    context.put("nVertices", commonVertexMasterContextWrapper.getTotalNumVerticesWrapper());
    context.put("nEdges", commonVertexMasterContextWrapper.getTotalNumEdgesWrapper());
    
    context.put("aggregators", commonVertexMasterContextWrapper.getPreviousAggregatedValues());
    
    List<Config> configs = new ArrayList<>();
    if (commonVertexMasterContextWrapper.getConfig() != null) {
      for (Iterator<Entry<String,String>> configIter =
        commonVertexMasterContextWrapper.getConfig().iterator(); configIter.hasNext(); ) {
        Entry<String,String> entry = configIter.next();
        configs.add(new Config(entry.getKey(), entry.getValue()));
      }
    }
    context.put("configs", configs);

    context.put("vertexId", giraphScenarioWrapper.getContextWrapper().getVertexIdWrapper());
    context.put("vertexValue", giraphScenarioWrapper.getContextWrapper().getVertexValueBeforeWrapper());
    context.put("vertexValueAfter", giraphScenarioWrapper.getContextWrapper().getVertexValueAfterWrapper());
    context.put("inMsgs", giraphScenarioWrapper.getContextWrapper().getIncomingMessageWrappers());
    context.put("neighbors", giraphScenarioWrapper.getContextWrapper().getNeighborWrappers());

    HashMap<OutgoingMessageWrapper, OutMsg> outMsgMap = new HashMap<>();
    for (OutgoingMessageWrapper msg : 
        (Collection<OutgoingMessageWrapper>)giraphScenarioWrapper.getContextWrapper().getOutgoingMessageWrappers()) {
      if (outMsgMap.containsKey(msg))
        outMsgMap.get(msg).incrementTimes();
      else
        outMsgMap.put(msg, new OutMsg(msg));
    }
    context.put("outMsgs", outMsgMap.values());
    
    return context;
  }
  
  public class FormatHelper {
    
    private DecimalFormat decimalFormat = new DecimalFormat("#.#####");
    
    public String formatWritable(Writable writable) {
      if (writable instanceof NullWritable) {
        return "NullWritable.get()";
      } else if (writable instanceof BooleanWritable) {
        return String.format("new BooleanWritable(%s)", format(((BooleanWritable) writable).get()));
      } else if (writable instanceof ByteWritable) {
        return String.format("new ByteWritable(%s)", format(((ByteWritable) writable).get()));
      } else if (writable instanceof IntWritable) {
        return String.format("new IntWritable(%s)", format(((IntWritable) writable).get()));
      } else if (writable instanceof LongWritable) {
        return String.format("new LongWritable(%s)", format(((LongWritable) writable).get()));
      } else if (writable instanceof FloatWritable) {
        return String.format("new FloatWritable(%s)", format(((FloatWritable) writable).get()));
      } else if (writable instanceof DoubleWritable) {
        return String.format("new DoubleWritable(%s)", format(((DoubleWritable) writable).get()));
      } else if (writable instanceof Text) {
        return String.format("new Text(%s)", ((Text) writable).toString());
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
        double val = ((Double) input).doubleValue();
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
