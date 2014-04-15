package stanford.infolab.debugger.plugin.handler;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.jdt.core.Flags;
import org.eclipse.jdt.core.ICompilationUnit;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.core.JavaModelException;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.handlers.HandlerUtil;

public class GenerateUnitTestHandler extends AbstractHandler {
	
	/**
	 * The constructor.
	 */
	public GenerateUnitTestHandler() {
	}

	/**
	 * the command has been executed, so extract extract the needed information
	 * from the application context.
	 */
	public Object execute(ExecutionEvent event) throws ExecutionException {
		ISelection sel = HandlerUtil.getCurrentSelection(event);
		if (sel != null && sel instanceof IStructuredSelection) {
			Object firstElement = ((IStructuredSelection)sel).getFirstElement();
			IWorkbenchWindow window = HandlerUtil.getActiveWorkbenchWindowChecked(event);
			
			if (firstElement instanceof ICompilationUnit) {
				ICompilationUnit javaFile = (ICompilationUnit)firstElement;
				
				try {
					// create import (TODO: add the library to the build path, that's a challenge)
		    		javaFile.createImport("org.junit.*", null, null);
		    		
		    		// create method
					for (IType type : javaFile.getTypes()) {
						if (Flags.isPublic(type.getFlags())) {
							if (!type.getMethod("foo", new String[] {}).exists()) {
								try {
									type.createMethod("public void foo() {assert (true);}", null, false, null);
								} catch (JavaModelException e) {
									throw new ExecutionException(e.getMessage(), e);
								}
							}
						}
					}
				} catch (JavaModelException e) {
					throw new ExecutionException(e.getMessage(), e);
				}
		    } else {
		    	MessageDialog.openInformation(window.getShell(), "Info",
		    			"Please select a Java source file");
		    }
		}
		return null;
	}
	
}
