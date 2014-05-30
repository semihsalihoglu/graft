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
    boolean isIdFloatingPoint = false;
    boolean isValueFloatingPoint = false;
    for (int i = 0; i < inputStrs.length; i++) {
      tokens[i] = inputStrs[i].trim().split("\\s+");
      if (tokens[i].length >= 1)
        isIdFloatingPoint |= isFloatingPoint(tokens[i][0]);
      if (tokens[i].length >= 2)
        isValueFloatingPoint |= isFloatingPoint(tokens[i][1]);
      for (int j = 2; j < tokens[i].length; j++) {
        isIdFloatingPoint |= isFloatingPoint(tokens[i][j]);
      }
    }
    
    Map<Object, TemplateVertex> vertexMap = new LinkedHashMap<>(inputStrs.length);
    for (int i = 0; i < inputStrs.length; i++) {
      Object id = convertToSuitableType(tokens[i][0], isIdFloatingPoint);
      Object value = convertToSuitableType(tokens[i][1], isValueFloatingPoint);
      TemplateVertex vertex = vertexMap.get(id);
      if (vertex == null) {
        vertex = new TemplateVertex(id);
        vertexMap.put(id, vertex);
      }
      vertex.setValue(value);

      if (tokens[i].length > 2) {
        for (int j = 2; j < tokens[i].length; j++) {
          Object nbrId = convertToSuitableType(tokens[i][j], isIdFloatingPoint);
          if (!vertexMap.containsKey(nbrId)) {
            vertexMap.put(nbrId, new TemplateVertex(nbrId));
          }
          vertex.addNeighbor(nbrId);
        }
      }
    }
    context.put("vertexIdClass", (isIdFloatingPoint ? DoubleWritable.class.getSimpleName()
        : LongWritable.class.getSimpleName()));
    context.put("vertexValueClass", (isValueFloatingPoint ? DoubleWritable.class.getSimpleName()
        : LongWritable.class.getSimpleName()));
    context.put("vertices", vertexMap);
    
    return context;
  }
  
  private boolean isFloatingPoint(String str) {
    try {
      Long.valueOf(str);
      return false;
    } catch (NumberFormatException ex) {
      return true;
    }
  }
  
  private String convertToSuitableType(String token, boolean isFloatingPoint) {
    return (isFloatingPoint ? Double.valueOf(token).toString() + "d" :
      Long.valueOf(token).toString() + "l");
  }
  
  public static class TemplateVertex {
    private Object id;
    private Object value;
    private ArrayList<Object> neighbors;
    
    public TemplateVertex(Object id) {
      super();
      this.id = id;
      this.neighbors = new ArrayList<>();
    }
    
    public Object getId() {
      return id;
    }
    
    public Object getValue() {
      return value;
    }
    
    public void setValue(Object value) {
      this.value = value;
    }
    
    public ArrayList<Object> getNeighbors() {
      return neighbors;
    }
    
    public void addNeighbor(Object nbrId) {
      neighbors.add(nbrId);
    }
  }
}
