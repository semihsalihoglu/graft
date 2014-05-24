package stanford.infolab.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

/**
 * The code generator to generate the end-to-end test case
 * @author Brian Truong Ba Quan
 */
public class TestGraphGenerator {
  
  public TestGraphGenerator() {
    Velocity.setProperty(VelocityEngine.RESOURCE_LOADER, "class");
    Velocity.setProperty("class." + VelocityEngine.RESOURCE_LOADER + ".class", 
        ClasspathResourceLoader.class.getName());
    Velocity.init();
  }
  
  public String generate(String[] inputStrs) throws IOException {
    VelocityContext context = buildContext(inputStrs);
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("TestGraph.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }
  
  private VelocityContext buildContext(String[] inputStrs) {
    VelocityContext context = new VelocityContext();
    // Parse the string and check whether the inputs are integers or floating-point numbers
    String[][] tokens = new String[inputStrs.length][];
    boolean isFloatingPoint = false;
    for (int i = 0; i < inputStrs.length; i++) {
      tokens[i] = inputStrs[i].trim().split("\\s+");
      for (int j = 0; j < tokens[i].length; j++) {
        try {
          Long.valueOf(tokens[i][0]);
        } catch (NumberFormatException ex) {
          isFloatingPoint = true;
        }
      }
    }
    
    Map<Object, TemplateVertex> vertexMap = new LinkedHashMap<>(inputStrs.length);
    for (int i = 0; i < inputStrs.length; i++) {
      Object id = readId(tokens[i][0], isFloatingPoint);
      TemplateVertex vertex = vertexMap.get(id);
      if (vertex == null) {
        vertex = new TemplateVertex(id);
        vertexMap.put(id, vertex);
      }
      if (tokens[i].length > 1) {
        for (int j = 1; j < tokens[i].length; j++) {
          Object nbrId = readId(tokens[i][j], isFloatingPoint);
          if (!vertexMap.containsKey(nbrId)) {
            vertexMap.put(nbrId, new TemplateVertex(nbrId));
          }
          vertex.addNeighbor(nbrId);
        }
      }
    }
    if (isFloatingPoint) {
      context.put("vertexIdClass", DoubleWritable.class.getSimpleName());
    } else {
      context.put("vertexIdClass", LongWritable.class.getSimpleName());
    }
    context.put("vertices", vertexMap);
    
    return context;
  }
  
  private String readId(String token, boolean isFloatingPoint) {
    if (isFloatingPoint) {
      return Double.valueOf(token).toString() + "d";
    } else {
      return Long.valueOf(token).toString() + "l";
    }
  }
  
  public static class TemplateVertex {
    private Object id;
    private ArrayList<Object> neighbors;
    
    public TemplateVertex(Object id) {
      super();
      this.id = id;
      this.neighbors = new ArrayList<>();
      
    }
    
    public Object getId() {
      return id;
    }
    
    public ArrayList<Object> getNeighbors() {
      return neighbors;
    }
    
    public void addNeighbor(Object nbrId) {
      neighbors.add(nbrId);
    }
  }
  
  public static void main(String[] args) {
    String[] graph = new String[] {
        "1 4 2 3",
        "2 1",
        "4 3 2",
        "5 2 4"
      };
    TestGraphGenerator generator = new TestGraphGenerator();
    try {
      String testGraphCode = generator.generate(graph);
      System.out.println(testGraphCode);
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }
}
