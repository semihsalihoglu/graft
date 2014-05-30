package org.apache.giraph.debugger.mock;

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

/**
 * The code generator to generate the end-to-end test case.
 * 
 * @author Brian Truong Ba Quan
 */
public class TestGraphGenerator extends VelocityBasedGenerator {
    
  public String generate(String[] inputStrs) throws IOException {
    VelocityContext context = buildContext(inputStrs);
    
    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("TestGraphTemplate.vm");
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
    context.put("vertexIdClass", (isFloatingPoint ? DoubleWritable.class.getSimpleName()
        : LongWritable.class.getSimpleName()));
    context.put("vertices", vertexMap);
    
    return context;
  }
  
  private String readId(String token, boolean isFloatingPoint) {
    return (isFloatingPoint ? Double.valueOf(token).toString() + "d" :
      Long.valueOf(token).toString() + "l");
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
}
