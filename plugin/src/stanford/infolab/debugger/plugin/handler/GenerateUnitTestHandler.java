package stanford.infolab.debugger.plugin.handler;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.core.commands.IHandler;
import org.eclipse.core.resources.IFile;
import org.eclipse.jdt.core.Flags;
import org.eclipse.jdt.core.ICompilationUnit;
import org.eclipse.jdt.core.IJavaElement;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.JavaModelException;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.IViewPart;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.handlers.HandlerUtil;
import org.eclipse.ui.part.FileEditorInput;

public class GenerateUnitTestHandler extends AbstractHandler {

	/**
	 * The constructor.
	 */
	public GenerateUnitTestHandler() {
	}

	/**
	 * The event run when the command is executed
	 * 
	 * @see IHandler#execute(ExecutionEvent)
	 * @return Must be null following {@link IHandler#execute(ExecutionEvent)}
	 */
	public Object execute(ExecutionEvent event) throws ExecutionException {
		IWorkbenchWindow window = HandlerUtil
				.getActiveWorkbenchWindowChecked(event);
		
		// Get the active site (e.g. view, editor, etc.)
		IWorkbenchPart activePart = HandlerUtil.getActivePart(event);
		
		if (activePart instanceof IEditorPart) {
			IEditorInput input = ((IEditorPart)activePart).getEditorInput();
			if (input instanceof FileEditorInput) {
				IFile file = ((FileEditorInput) input).getFile();
				IJavaElement javaElement = JavaCore.create(file);
				if (javaElement instanceof ICompilationUnit) {
					try {
						createUnitTestCase((ICompilationUnit)javaElement);
					} catch (JavaModelException e) {
						throw new ExecutionException(e.getMessage(), e);
					}
				} else {
				  // Not a Java file. Show a message to inform users.
					displayErrorMessage(window, "It is not a Java editor!");
				}
			}

		} else if (activePart instanceof IViewPart) {
			ISelection sel = HandlerUtil.getCurrentSelection(event);
			// only process single-file structured selection
			if (sel != null && sel instanceof IStructuredSelection 
					&& ((IStructuredSelection)sel).size() == 1) {
				Object firstElement = ((IStructuredSelection) sel).getFirstElement();
	
				// Is the element a Java file?
				if (firstElement instanceof ICompilationUnit) {
					// Yes. Generate the unit test cases within it
					ICompilationUnit javaFile = (ICompilationUnit) firstElement;
					try {
						createUnitTestCase(javaFile);
					} catch (JavaModelException e) {
						throw new ExecutionException(e.getMessage(), e);
					}		
				} else {
					// Not a Java file. Show a message to inform users.				
					displayErrorMessage(window, "Non-Java file is being selected.");
				}
			}
		}

		return null;
	}
	
	/**
	 * Create a method named foo() and an import to org.jnit.* within a .java file
	 * @param javaFile The input Java source file (.java)
	 * @throws JavaModelException 
	 */
	private void createUnitTestCase(ICompilationUnit javaFile) throws JavaModelException {
		// Create import
		javaFile.createImport(
		    "org.junit.*",
		    null, // don't care about the position of the import
		    null // don't track the progress of creating import
		    );

		// Create the test case method in the public class of this Java file
		// Scan through all classes and find the public class
		for (IType type : javaFile.getTypes()) {
			if (Flags.isPublic(type.getFlags())) {
				// Check whether methods of the same signature exist
				if (!type.getMethod("foo", new String[] {}).exists()) {
					String methodStr = "public void foo() {assert (true);}";
					type.createMethod(methodStr, // the added method
					    null, // don't care where the method is added
					    false, // don't replace user's method by our method
					    null // don't monitor the generation process
					);
				}
				break; // a Java file has at most public class
			}
		}
	}

	/**
	 * Display an error message in Eclipse
	 * SUGGESTION(brian) This static method could be put somewhere else
	 * @param window The Eclipse window
	 * @param msg The message
	 */
	public static void displayErrorMessage(IWorkbenchWindow window, String msg) {
		MessageDialog.openError(window.getShell(), "Error", msg);
	}
}
