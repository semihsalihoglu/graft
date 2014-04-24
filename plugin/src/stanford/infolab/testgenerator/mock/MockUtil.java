package stanford.infolab.debugger.testgenerator.mock;

import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import stanford.infolab.debugger.testgenerator.scenario.IScenario;
import static org.mockito.Mockito.*;

/**
 * A utility class to mock.
 * 
 * @author Brian Truong
 */
public final class MockUtil {

  @SuppressWarnings("rawtypes")
  public static
      <I extends WritableComparable, V extends Writable, E extends Writable, M1 extends Writable, 
          M2 extends Writable> Computation<I, V, E, M1, M2> registerComputation(
              final Computation<I, V, E, M1, M2> mockComp, Vertex<I, V, E> mockVertex, 
              final IScenario<I, V, E, M1, M2> scenario) {
    try {
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          for (I neighborId : scenario.getNeighbors()) {
            for (M2 msg : scenario.getOutgoingMessages(neighborId)) {
              mockComp.sendMessage(neighborId, msg);
            }
          }
          return null;
        }
      }).when(mockComp).compute(mockVertex, scenario.getIncomingMessages());
    } catch (Exception e) {
      e.printStackTrace();
    };

    return mockComp;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <I extends WritableComparable, V extends Writable, E extends Writable>
      Vertex<I, V, E> createMockVertex(I vertexId, V vertexValue) {
    Vertex<I, V, E> mockVertex = mock(Vertex.class);
    when(mockVertex.getId()).thenReturn(vertexId);
    when(mockVertex.getValue()).thenReturn(vertexValue);
    return mockVertex;
  }
}
