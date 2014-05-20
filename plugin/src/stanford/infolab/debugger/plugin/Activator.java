package stanford.infolab.debugger.plugin;

import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;

/**
 * The activator class controls the plug-in life cycle
 */
public class Activator extends AbstractUIPlugin {

  // The plug-in ID
  public static final String PLUGIN_ID = "stanford.infolab.plugin.GiraphDebugger"; //$NON-NLS-1$

  private static final IPath ICONS_PATH = new Path("$nl$/icons/full"); //$NON-NLS-1$

  // The shared instance
  private static Activator plugin;

  /**
   * The constructor
   */
  public Activator() {}

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.plugin.AbstractUIPlugin#start(org.osgi.framework.BundleContext)
   */
  public void start(BundleContext context) throws Exception {
    super.start(context);
    plugin = this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.plugin.AbstractUIPlugin#stop(org.osgi.framework.BundleContext)
   */
  public void stop(BundleContext context) throws Exception {
    plugin = null;
    super.stop(context);
  }

  /**
   * Returns the shared instance
   * 
   * @return the shared instance
   */
  public static Activator getDefault() {
    return plugin;
  }

  public static ImageDescriptor getImageDescriptor(String relativePath) {
    IPath path = ICONS_PATH.append(relativePath);
    return createImageDescriptor(getDefault().getBundle(), path, true);
  }

  /**
   * Creates an image descriptor for the given path in a bundle. The path can contain variables like
   * $NL$. If no image could be found, <code>useMissingImageDescriptor</code> decides if either the
   * 'missing image descriptor' is returned or <code>null</code>.
   * 
   * @param bundle a bundle
   * @param path path in the bundle
   * @param useMissingImageDescriptor if <code>true</code>, returns the shared image descriptor for
   *        a missing image. Otherwise, returns <code>null</code> if the image could not be found
   * @return an {@link ImageDescriptor}, or <code>null</code> iff there's no image at the given
   *         location and <code>useMissingImageDescriptor</code> is <code>true</code>
   */
  private static ImageDescriptor createImageDescriptor(Bundle bundle, IPath path,
      boolean useMissingImageDescriptor) {
    URL url = FileLocator.find(bundle, path, null);
    if (url != null) {
      return ImageDescriptor.createFromURL(url);
    }
    if (useMissingImageDescriptor) {
      return ImageDescriptor.getMissingImageDescriptor();
    }
    return null;
  }
}
