package stanford.infolab.debugger.testgenerator.scenario;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This interface represents a manager object which can load and save scenario data.
 * 
 * @param <I> Vertex ID
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M1> Incoming messages
 * @param <M2> Outgoing messages
 * 
 * @author Brian Truong
 */
@SuppressWarnings("rawtypes")
public interface IScenarioManager<I extends WritableComparable, V extends Writable, 
    E extends Writable, M1 extends Writable, M2 extends Writable> {

  /**
   * Load the list of scenarios stored in the specified file.
   * 
   * @param fileName The name of the file
   * @return The list of scenarios
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public ScenarioList<IScenario<I, V, E, M1, M2>> load(String fileName)
      throws ClassNotFoundException, IOException;

  /**
   * Save the list of scenario into the specified file. All existing data are overwritten.
   * 
   * @param fileName The name of the file
   * @param scenarioList The list of scenarios
   * @throws IOException
   */
  public void save(String fileName, ScenarioList<IScenario<I, V, E, M1, M2>> scenarioList)
      throws IOException;
}
