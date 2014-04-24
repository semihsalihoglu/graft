package stanford.infolab.debugger.testgenerator.scenario;

import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.graph.Computation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This object encapsulates a list of test scenarios for a particular class.
 * @param   <T> The type of the test scenarios
 * @author  Brian Truong
 */
@SuppressWarnings("rawtypes")
public class ScenarioList<T extends IScenario<? extends WritableComparable, ? extends Writable, 
    ? extends Writable, ? extends Writable, ? extends Writable>> {

  private Class<? extends Computation<?,?,?,?,?>> classUnderTest;
  private Class<? extends WritableComparable> vertexIdClass;
  private Class<? extends Writable> vertexValueClass;
  private Class<? extends Writable> edgeValueClass;
  private Class<? extends Writable> incomingMessageClass;
  private Class<? extends Writable> outgoingMessageClass;
  
  public Class<? extends Computation<?, ?, ?, ?, ?>> getClassUnderTest() {
    return classUnderTest;
  }
  
  public Class<? extends WritableComparable> getVertexIdClass() {
    return vertexIdClass;
  }

  public Class<? extends Writable> getVertexValueClass() {
    return vertexValueClass;
  }

  public Class<? extends Writable> getEdgeValueClass() {
    return edgeValueClass;
  }

  public Class<? extends Writable> getIncomingMessageClass() {
    return incomingMessageClass;
  }

  public Class<? extends Writable> getOutgoingMessageClass() {
    return outgoingMessageClass;
  }

  private ArrayList<T> testScenarios = new ArrayList<>();
  
  public List<T> getScenarios() {
    return testScenarios;
  }

  public void addScenario(T testScenario) {
    this.testScenarios.add(testScenario);
  }
  
  public ScenarioList(Class<? extends Computation<?,?,?,?,?>> classUnderTest, 
      Class<? extends WritableComparable> vertexIdClass, 
      Class<? extends Writable> vertexValueClass, 
      Class<? extends Writable> edgeValueClass, 
      Class<? extends Writable> incomingMessageClass, 
      Class<? extends Writable> outgoingMessageClass) {
    this.classUnderTest = classUnderTest;
    this.vertexIdClass = vertexIdClass;
    this.vertexValueClass = vertexValueClass;
    this.edgeValueClass = edgeValueClass;
    this.incomingMessageClass = incomingMessageClass;
    this.outgoingMessageClass = outgoingMessageClass;
  }
}
