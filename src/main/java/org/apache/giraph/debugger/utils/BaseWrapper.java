package org.apache.giraph.debugger.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/** 
 * Base class for all wrapper classes that wrap a protobuf.
 * 
 * @author semihsalihoglu
 */
public abstract class BaseWrapper {

  @SuppressWarnings("unchecked")
  protected <U> Class<U> castClassToUpperBound(Class<?> clazz, Class<U> upperBound) {
    if (!upperBound.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("The class " + clazz.getName() + " is not a subclass of "
          + upperBound.getName());
    }
    return (Class<U>) clazz;
  }

  void fromByteString(ByteString byteString, Writable writable) {
    if (writable != null) {
      WritableUtils.readFieldsFromByteArray(byteString.toByteArray(), writable);
    }
  }
  
  ByteString toByteString(Writable writable) {
    return ByteString.copyFrom(WritableUtils.writeToByteArray(writable));
  }
  
  public void save(String fileName) throws IOException {
    try (FileOutputStream output = new FileOutputStream(fileName)) {
      buildProtoObject().writeTo(output);
      output.close();
    }
  }

  public void saveToHDFS(FileSystem fs, String fileName) throws IOException {
    Path pt = new Path(fileName);
    buildProtoObject().writeTo(fs.create(pt, true).getWrappedStream());
  }
  
  public abstract GeneratedMessage buildProtoObject();

  public void load(String fileName) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
    loadFromProto(parseProtoFromInputStream(new FileInputStream(fileName)));
  }

  public void loadFromHDFS(FileSystem fs, String fileName) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException {
    loadFromProto(parseProtoFromInputStream(fs.open(new Path(fileName))));
  }
  
  public abstract GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException ;

  public abstract void loadFromProto(GeneratedMessage protoObject) throws ClassNotFoundException,
    IOException, InstantiationException, IllegalAccessException;
}
