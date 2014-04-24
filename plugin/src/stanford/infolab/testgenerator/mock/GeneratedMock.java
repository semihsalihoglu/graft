package stanford.infolab.debugger.testgenerator.mock;

import java.io.IOException;

import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import stanford.infolab.debugger.testgenerator.scenario.IScenario;
import stanford.infolab.debugger.testgenerator.scenario.IScenarioManager;
import stanford.infolab.debugger.testgenerator.scenario.ProtoBufScenarioManager;
import stanford.infolab.debugger.testgenerator.scenario.ScenarioList;
import static org.mockito.Mockito.*;

/**
 * This class is the template for the generated code to mock the computation. The ClassUnderTest and
 * its type parameters shall be replaced by suitable names from the scenario file.
 * 
 * @author Brian Truong
 */
public class GeneratedMock {

  private abstract static class ClassUnderTest implements
      Computation<LongWritable, DoubleWritable, NullWritable, DoubleWritable, DoubleWritable> {
  }

  public static void main(String[] args) {
    try {
      IScenarioManager<LongWritable, DoubleWritable, NullWritable, DoubleWritable, DoubleWritable> 
          manager = new ProtoBufScenarioManager<>();
      ScenarioList<IScenario<LongWritable, DoubleWritable, NullWritable, DoubleWritable, 
          DoubleWritable>> scenarioList = manager.load("test");

      for (IScenario<LongWritable, DoubleWritable, NullWritable, DoubleWritable, DoubleWritable> 
          scenario : scenarioList.getScenarios()) {
        Computation<LongWritable, DoubleWritable, NullWritable, DoubleWritable, DoubleWritable> 
            mock = mock(ClassUnderTest.class);
        Vertex<LongWritable, DoubleWritable, NullWritable> mockVertex =
            MockUtil.createMockVertex(scenario.getVertexId(), scenario.getVertexValue());
        try {
          mock = MockUtil.registerComputation(mock, mockVertex, scenario);
          mock.compute(mockVertex, scenario.getIncomingMessages());
          verify(mock, times(2)).sendMessage(eq(new LongWritable(2L)), any(DoubleWritable.class));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
    }
  }

}
