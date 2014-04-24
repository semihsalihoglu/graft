package stanford.infolab.debugger.testgenerator.scenario;

import org.apache.giraph.examples.PageRankComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;

/**
 * A main class to demonstrate how to save and load scenarios.
 * @author Brian Truong
 *
 */
public final class ScenarioTest {
  
  /**
   * Hide default constructor.
   */
  private ScenarioTest() { }
  
  private static void save(String fileName, IScenarioManager<LongWritable, DoubleWritable, 
      NullWritable, DoubleWritable, DoubleWritable> manager) throws IOException {
    
    ScenarioList<IScenario<LongWritable, DoubleWritable, NullWritable, DoubleWritable, 
        DoubleWritable>> scenarioList = new ScenarioList<>(PageRankComputation.class, 
            LongWritable.class, DoubleWritable.class, NullWritable.class, DoubleWritable.class, 
            DoubleWritable.class);
    DefaultScenario<LongWritable, DoubleWritable, NullWritable, DoubleWritable, 
        DoubleWritable> scenario = new DefaultScenario<>();
    scenario.setVertexId(new LongWritable(1L));
    scenario.setVertexValue(new DoubleWritable(1.0));
    scenario.addNeighbor(new LongWritable(2L), NullWritable.get());
    scenario.addOutgoingMessage(new LongWritable(2L), new DoubleWritable(0.2));
    scenario.addOutgoingMessage(new LongWritable(2L), new DoubleWritable(0.3));
    scenario.addOutgoingMessage(new LongWritable(3L), new DoubleWritable(0.4));
    scenario.addIncomingMessage(new DoubleWritable(0.3));
    scenario.addIncomingMessage(new DoubleWritable(0.4));
    scenario.addIncomingMessage(new DoubleWritable(0.3));
    scenarioList.addScenario(scenario);
    
    manager.save(fileName, scenarioList);
  }
  
  private static void load(String fileName, IScenarioManager<LongWritable, DoubleWritable, 
      NullWritable, DoubleWritable, DoubleWritable> manager) 
          throws ClassNotFoundException, IOException {
    
    ScenarioList<IScenario<LongWritable, DoubleWritable, NullWritable, DoubleWritable, 
        DoubleWritable>> scenarioList =
        manager.load(fileName);
    
    System.out.println(scenarioList.getClassUnderTest());
    System.out.println(scenarioList.getVertexIdClass());
    System.out.println(scenarioList.getVertexValueClass());
    System.out.println(scenarioList.getEdgeValueClass());
    System.out.println(scenarioList.getIncomingMessageClass());
    System.out.println(scenarioList.getOutgoingMessageClass());
    
    for (IScenario<LongWritable, DoubleWritable, NullWritable, DoubleWritable, DoubleWritable> 
        scenario : scenarioList.getScenarios()) {
      System.out.println(scenario.getVertexId().get() + ":" + scenario.getVertexValue().get());
      System.out.println(scenario.getNeighbors().size());
      for (LongWritable neighborId : scenario.getNeighbors()) {
        System.out.println("Neighbor: " + neighborId + ":" + scenario.getEdgeValue(neighborId));
        for (DoubleWritable msg : scenario.getOutgoingMessages(neighborId)) {
          System.out.println("out msg = " + msg);
        }
      }
      for (DoubleWritable msg : scenario.getIncomingMessages()) {
        System.out.println("in msg = " + msg);
      }
    }
  }

  public static void main(String[] args) {
    try {
      IScenarioManager<LongWritable, DoubleWritable, NullWritable, DoubleWritable, DoubleWritable> 
          manager = new ProtoBufScenarioManager<>();
      save("test", manager);
      load("test", manager);
    } catch (ClassNotFoundException|IOException e) {
      e.printStackTrace();
    }
  }
}
