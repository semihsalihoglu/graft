package org.apache.giraph.debugger.mock;

import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.RuntimeConstants;

class VelocityBasedGenerator {

  protected VelocityBasedGenerator() {
    Velocity.setProperty(RuntimeConstants.RESOURCE_LOADER, "class");
    Velocity.setProperty(
      "class." + RuntimeConstants.RESOURCE_LOADER + ".class",
      PrefixedClasspathResourceLoader.class.getName());
    Velocity.setProperty("class." + RuntimeConstants.RESOURCE_LOADER +
      ".prefix", "/" +
      getClass().getPackage().getName().replaceAll("\\.", "/") + "/");
    Velocity.init();
  }

}
