/**
 * 
 */
package stanford.infolab.debugger.plugin;

import org.eclipse.core.runtime.Status;
import org.eclipse.ui.statushandlers.AbstractStatusHandler;
import org.eclipse.ui.statushandlers.StatusManager;
import org.eclipse.ui.statushandlers.WorkbenchErrorHandler;

/**
 * This static class is the centralized place to log all messages (errors, warnings, etc.) of our
 * plugin. It extracts some plugin-specific parameters, packages them into the messages and sends
 * them to Eclipse. Note that this class does not specific how the messages are handled/displayed.
 * 
 * To specify how the messages are displayed, extend {@link AbstractStatusHandler} and then register
 * the handler to org.eclipse.ui.statusHandlers extension point. Without a specific handler, by
 * default, {@link WorkbenchErrorHandler} is used.
 * 
 * @author Brian Truong Ba Quan
 */
public final class StatusLogger {

  public static final void logWarning(String msg) {
    StatusManager.getManager().handle(new Status(Status.WARNING, Activator.PLUGIN_ID, msg));
  }
  
  public static final void logError(String msg) {
    StatusManager.getManager().handle(new Status(Status.ERROR, Activator.PLUGIN_ID, msg));
  }
  
  public static final void logError(String msg, Throwable ex) {
    StatusManager.getManager().handle(new Status(Status.ERROR, Activator.PLUGIN_ID, msg, ex));
  }
  
  public static final void logError(Throwable ex) {
    StatusManager.getManager().handle(new Status(Status.ERROR, Activator.PLUGIN_ID, ex.getMessage(), ex));
  }
}
