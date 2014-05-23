package stanford.infolab.debugger.utils;

import org.apache.hadoop.io.WritableComparable;

/**
 * Base wrapper class for {@link GiraphVertexScenarioWrapper}, {@link MsgIntegrityViolationWrapper},
 * {@link VertexValueIntegrityViolationWrapper}.
 * @author semihsalihoglu
 *
 * @param <I> Vertex id 
 */
@SuppressWarnings("rawtypes")
public abstract class BaseScenarioAndIntegrityWrapper<I extends WritableComparable> extends BaseWrapper {
  Class<I> vertexIdClass;

  BaseScenarioAndIntegrityWrapper() {};

  public BaseScenarioAndIntegrityWrapper(Class<I> vertexIdClass) {
    initialize(vertexIdClass);
  }

  public Class<I> getVertexIdClass() {
    return vertexIdClass;
  }

  public void initialize(Class<I> vertexIdClass) {
    this.vertexIdClass = vertexIdClass;
  }

  @Override
  public String toString() {
    return "\nvertexIdClass: " + getVertexIdClass().getCanonicalName();
  }  
}
