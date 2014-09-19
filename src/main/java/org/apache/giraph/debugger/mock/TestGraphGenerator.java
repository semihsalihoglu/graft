package org.apache.giraph.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

/**
 * The code generator to generate the end-to-end test case.
 * 
 * @author Brian Truong Ba Quan
 */
public class TestGraphGenerator extends VelocityBasedGenerator {

  private enum WritableType {
    NULL, LONG, DOUBLE
  };

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
    context.put("helper", new FormatHelper());
    // Parse the string and check whether the inputs are integers or
    // floating-point numbers
    String[][] tokens = new String[inputStrs.length][];
    WritableType idWritableType = WritableType.NULL;
    WritableType valueWritableType = WritableType.NULL;
    WritableType edgeValueWritableType = WritableType.NULL;
    for (int i = 0; i < inputStrs.length; i++) {
      tokens[i] = inputStrs[i].trim().split("\\s+");
      String[] nums = tokens[i][0].split(":");
      WritableType type;
      idWritableType =
        ((type = parseWritableType(nums[0])).ordinal() > idWritableType
          .ordinal() ? type : idWritableType);
      if (nums.length > 1)
        valueWritableType =
          ((type = parseWritableType(nums[1])).ordinal() > valueWritableType
            .ordinal() ? type : valueWritableType);

      for (int j = 1; j < tokens[i].length; j++) {
        nums = tokens[i][j].split(":");
        idWritableType =
          ((type = parseWritableType(nums[0])).ordinal() > idWritableType
            .ordinal() ? type : idWritableType);
        if (nums.length > 1)
          edgeValueWritableType =
            ((type = parseWritableType(nums[1])).ordinal() > edgeValueWritableType
              .ordinal() ? type : edgeValueWritableType);
      }
    }

    Map<Object, TemplateVertex> vertexMap =
      new LinkedHashMap<>(inputStrs.length);
    String str;
    for (int i = 0; i < inputStrs.length; i++) {
      String[] nums = tokens[i][0].split(":");
      Object id = convertToSuitableType(nums[0], idWritableType);
      str = nums.length > 1 ? nums[1] : "0";
      Object value = convertToSuitableType(str, valueWritableType);
      TemplateVertex vertex = vertexMap.get(id);
      if (vertex == null) {
        vertex = new TemplateVertex(id);
        vertexMap.put(id, vertex);
      }
      vertex.setValue(value);

      for (int j = 1; j < tokens[i].length; j++) {
        nums = tokens[i][j].split(":");
        Object nbrId = convertToSuitableType(nums[0], idWritableType);
        str = nums.length > 1 ? nums[1] : "0";
        Object edgeValue = convertToSuitableType(str, edgeValueWritableType);
        if (!vertexMap.containsKey(nbrId)) {
          vertexMap.put(nbrId, new TemplateVertex(nbrId));
        }
        vertex.addNeighbor(nbrId, edgeValue);
      }
    }

    updateContextByWritableType(context, "vertexIdClass", idWritableType);
    updateContextByWritableType(context, "vertexValueClass", valueWritableType);
    updateContextByWritableType(context, "edgeValueClass",
      edgeValueWritableType);
    context.put("vertices", vertexMap);

    return context;
  }

  private WritableType parseWritableType(String str) {
    if (str == null)
      return WritableType.NULL;
    else {
      try {
        Long.valueOf(str);
        return WritableType.LONG;
      } catch (NumberFormatException ex) {
        return WritableType.DOUBLE;
      }
    }
  }

  private void updateContextByWritableType(VelocityContext context,
    String contextKey, WritableType type) {
    switch (type) {
    case NULL:
      context.put(contextKey, NullWritable.class.getSimpleName());
      break;
    case LONG:
      context.put(contextKey, LongWritable.class.getSimpleName());
      break;
    case DOUBLE:
      context.put(contextKey, DoubleWritable.class.getSimpleName());
      break;
    default:
      throw new IllegalStateException("Unknown type!");
    }
  }

  private Writable convertToSuitableType(String token, WritableType type) {
    switch (type) {
    case NULL:
      return NullWritable.get();
    case LONG:
      return new LongWritable(Long.valueOf(token));
    case DOUBLE:
      return new DoubleWritable(Double.valueOf(token));
    default:
      throw new IllegalStateException("Unknown type!");
    }
  }

  public static class TemplateVertex {
    private Object id;
    private Object value;
    private ArrayList<TemplateNeighbor> neighbors;

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

    public ArrayList<TemplateNeighbor> getNeighbors() {
      return neighbors;
    }

    public void addNeighbor(Object nbrId, Object edgeValue) {
      neighbors.add(new TemplateNeighbor(nbrId, edgeValue));
    }
  }

  public static class TemplateNeighbor {
    private Object id;
    private Object edgeValue;

    public TemplateNeighbor(Object id, Object edgeValue) {
      super();
      this.id = id;
      this.edgeValue = edgeValue;
    }

    public Object getId() {
      return id;
    }

    public Object getEdgeValue() {
      return edgeValue;
    }
  }
}
