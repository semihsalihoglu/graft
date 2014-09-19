package org.apache.giraph.debugger.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  protected <U> Class<U> castClassToUpperBound(Class<?> clazz,
    Class<U> upperBound) {
    if (!upperBound.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("The class " + clazz.getName() +
        " is not a subclass of " + upperBound.getName());
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
    OutputStream wrappedStream = fs.create(pt, true).getWrappedStream();
    buildProtoObject().writeTo(wrappedStream);
    wrappedStream.close();
  }

  public abstract GeneratedMessage buildProtoObject();

  public void load(String fileName) throws ClassNotFoundException, IOException,
    InstantiationException, IllegalAccessException {
    loadFromProto(parseProtoFromInputStream(new FileInputStream(fileName)));
  }

  public void loadFromHDFS(FileSystem fs, String fileName)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    loadFromProto(parseProtoFromInputStream(fs.open(new Path(fileName))));
  }

  public abstract GeneratedMessage parseProtoFromInputStream(
    InputStream inputStream) throws IOException;

  public abstract void loadFromProto(GeneratedMessage protoObject)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException;

  /**
   * Add given URLs to the CLASSPATH before loading from HDFS. To do so, we hack
   * the system class loader, assuming it is an URLClassLoader.
   * 
   * XXX Setting the currentThread's context class loader has no effect on
   * Class#forName().
   * 
   * @see http://stackoverflow.com/a/12963811/390044
   * @param fs
   * @param fileName
   * @param classPaths
   * @throws ClassNotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IOException
   */
  public void loadFromHDFS(FileSystem fs, String fileName, URL... classPaths)
    throws ClassNotFoundException, InstantiationException,
    IllegalAccessException, IOException {
    for (URL url : classPaths) {
      addPath(url);
    }
    loadFromHDFS(fs, fileName);
  }

  /**
   * @param u
   *          the URL to add to the CLASSPATH
   * @see http://stackoverflow.com/a/252967/390044
   */
  private static void addPath(URL u) {
    // need to do add path to Classpath with reflection since the
    // URLClassLoader.addURL(URL url) method is protected:
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    if (cl instanceof URLClassLoader) {
      URLClassLoader urlClassLoader = (URLClassLoader) cl;
      Class<URLClassLoader> urlClass = URLClassLoader.class;
      try {
        Method method = urlClass.getDeclaredMethod("addURL",
          new Class[] { URL.class });
        method.setAccessible(true);
        method.invoke(urlClassLoader, new Object[] { u });
      } catch (NoSuchMethodException | SecurityException |
        IllegalAccessException | IllegalArgumentException |
        InvocationTargetException e) {
        throw new IllegalStateException("Cannot add URL to system ClassLoader",
          e);
      }
    } else {
      throw new IllegalStateException(
        "Cannot add URL to system ClassLoader of type " +
          cl.getClass().getSimpleName());
    }
  }
}
