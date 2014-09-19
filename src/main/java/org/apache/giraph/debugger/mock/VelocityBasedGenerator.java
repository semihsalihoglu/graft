package org.apache.giraph.debugger.mock;

import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;

class VelocityBasedGenerator {

  protected VelocityBasedGenerator() {
    Velocity.setProperty(VelocityEngine.RESOURCE_LOADER, "class");
    Velocity.setProperty("class." + VelocityEngine.RESOURCE_LOADER + ".class",
      PrefixedClasspathResourceLoader.class.getName());
    Velocity.setProperty("class." + VelocityEngine.RESOURCE_LOADER + ".prefix",
      "/" + getClass().getPackage().getName().replaceAll("\\.", "/") + "/");
    Velocity.init();
  }

}
