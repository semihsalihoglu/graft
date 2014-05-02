package mock;

import java.io.StringWriter;
import java.util.ArrayList;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

public class TestCaseGenerator {

  public String generateTest(Input input) {
    Velocity.init();

    VelocityContext context = new VelocityContext();

    context.put("classUnderTestName", new String(input.getClassUnderTest()));
    
    context.put("className", new String("TestedComputationTest"));
    context.put("vertexIdWrapper", input.getVertexIdWrapper());
    context.put("vertexValueWrapper", input.getVertexValueWrapper());
    context.put("edgeValueWrapper", input.getEdgeValueWrapper());
    context.put("inMsgWrapper", input.getIncomingMessageWrapper());
    context.put("outMsgWrapper", input.getOutgoingMessageWrapper());

    context.put("vertexId", input.getVertexId());
    context.put("vertexValue", input.getVertexValue());
    context.put("inMsgs", input.getIncomingMessages());

    ArrayList<Object> configs = new ArrayList<>();
    Config<Integer> config = new Config<Integer>("SUPERSTEP_COUNT", 10);
    configs.add(config);
    context.put("configs", input.getConfigs());
    context.put("superstep", input.getSuperstep());
    context.put("nVertices", input.getNVertices());
    context.put("nEdges", input.getNEdges());


    Template template = null;

    try {
      template = Velocity.getTemplate("TestCaseTemplate.vm");
    } catch (ResourceNotFoundException | ParseErrorException | MethodInvocationException e) {
      e.printStackTrace(System.err);
    }

    StringWriter sw = new StringWriter();

    template.merge(context, sw);

    return sw.toString();
  }
}
