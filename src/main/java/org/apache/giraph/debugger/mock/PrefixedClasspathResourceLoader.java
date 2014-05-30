package org.apache.giraph.debugger.mock;

import java.io.InputStream;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

/**
 * @see http://stackoverflow.com/a/9693749/390044
 */
public class PrefixedClasspathResourceLoader extends ClasspathResourceLoader {

  /** Prefix to be added to any names */
  private String prefix = "";

  @Override
  public void init(ExtendedProperties configuration) {
    prefix = configuration.getString("prefix", "");
  }

  @Override
  public InputStream getResourceStream(String name) throws ResourceNotFoundException {
    System.err.println(prefix + name);
    return super.getResourceStream(prefix + name);
  }

}