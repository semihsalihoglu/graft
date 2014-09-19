package org.apache.giraph.debugger.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.debugger.mock.ComputationComputeTestGenerator.Config;
import org.apache.giraph.debugger.utils.CommonVertexMasterContextWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexScenarioClassesWrapper;
import org.apache.velocity.VelocityContext;

public abstract class TestGenerator extends VelocityBasedGenerator {

  @SuppressWarnings("rawtypes")
  private Set<Class> complexWritables = new HashSet<>();

  @SuppressWarnings("rawtypes")
  public Set<Class> getComplexWritableList() {
    return complexWritables;
  }

  protected void resetComplexWritableList() {
    this.complexWritables.clear();
  }

  protected class ContextBuilder {

    protected VelocityContext context;

    public ContextBuilder() {
      context = new VelocityContext();
      addHelper();
      addWritableReadFromString();
    }

    public VelocityContext getContext() {
      return context;
    }

    private void addHelper() {
      context.put("helper", new FormatHelper(complexWritables));
    }

    private void addWritableReadFromString() {
      context.put("complexWritables", complexWritables);
    }

    public void addPackage(String testPackage) {
      context.put("package", testPackage);
    }

    @SuppressWarnings("rawtypes")
    public void addClassUnderTest(Class classUnderTest) {
      context.put("classUnderTestName", classUnderTest.getSimpleName());
    }

    public void addClassName(String className) {
      if (className == null) {
        context.put("className", context.get("classUnderTestName") + "Test");
      } else {
        context.put("className", className);
      }
    }

    @SuppressWarnings("rawtypes")
    public void addTestClassInfo(String testPackage, Class classUnderTest,
      String className) {
      addPackage(testPackage);
      addClassUnderTest(classUnderTest);
      addClassName(className);
    }

    @SuppressWarnings("rawtypes")
    public void addVertexScenarioClassesWrapper(
      VertexScenarioClassesWrapper vertexScenarioClassesWrapper) {
      HashSet<Class> usedTypes = new LinkedHashSet<>(6);
      usedTypes.add(vertexScenarioClassesWrapper.getClassUnderTest());
      usedTypes.add(vertexScenarioClassesWrapper.getVertexIdClass());
      usedTypes.add(vertexScenarioClassesWrapper.getVertexValueClass());
      usedTypes.add(vertexScenarioClassesWrapper.getEdgeValueClass());
      usedTypes.add(vertexScenarioClassesWrapper.getIncomingMessageClass());
      usedTypes.add(vertexScenarioClassesWrapper.getOutgoingMessageClass());
      context.put("usedTypes", usedTypes);
    }

    public void addCommonMasterVertexContext(
      CommonVertexMasterContextWrapper commonVertexMasterContextWrapper) {
      context.put("superstepNo",
        commonVertexMasterContextWrapper.getSuperstepNoWrapper());
      context.put("nVertices",
        commonVertexMasterContextWrapper.getTotalNumVerticesWrapper());
      context.put("nEdges",
        commonVertexMasterContextWrapper.getTotalNumEdgesWrapper());

      context.put("aggregators",
        commonVertexMasterContextWrapper.getPreviousAggregatedValues());

      List<Config> configs = new ArrayList<>();
      if (commonVertexMasterContextWrapper.getConfig() != null) {
        for (Iterator<Entry<String, String>> configIter =
          commonVertexMasterContextWrapper.getConfig().iterator(); configIter
          .hasNext();) {
          Entry<String, String> entry = configIter.next();
          configs.add(new Config(entry.getKey(), entry.getValue()));
        }
      }
      context.put("configs", configs);
    }
  }
}
