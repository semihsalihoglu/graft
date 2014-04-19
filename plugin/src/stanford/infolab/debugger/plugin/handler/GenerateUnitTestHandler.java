package stanford.infolab.debugger.plugin.handler;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.core.commands.IHandler;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.runtime.Status;
import org.eclipse.jdt.core.Flags;
import org.eclipse.jdt.core.ICompilationUnit;
import org.eclipse.jdt.core.IJavaElement;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.JavaModelException;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.IViewPart;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.handlers.HandlerUtil;
import org.eclipse.ui.part.FileEditorInput;

import stanford.infolab.debugger.plugin.StatusLogger;

/**
 * A handler to generate a Giraph unit test.
 * 
 * TODO(brian): Currently just puts empty foo() method. Add Giraph-specific code.
 * 
 * @author Brian Truong
 */
public class GenerateUnitTestHandler extends AbstractHandler {

  public GenerateUnitTestHandler() {}

  /**
   * The event run when the command is executed.
   * 
   * @see IHandler#execute(ExecutionEvent)
   * @return Must be null following {@link IHandler#execute(ExecutionEvent)}
   */
  public Object execute(ExecutionEvent event) throws ExecutionException {
    // Get the active site (e.g. view, editor, etc.).
    IWorkbenchPart activePart = HandlerUtil.getActivePart(event);

    if (activePart instanceof IEditorPart) {
      IEditorInput input = ((IEditorPart) activePart).getEditorInput();
      if (input instanceof FileEditorInput) {
        IFile file = ((FileEditorInput) input).getFile();
        IJavaElement javaElement = JavaCore.create(file);

        // Error occurs if the editor is not on a Java file (plugin.xml currently does not block
        // this path).
        createUnitTestCaseOrError(javaElement, Status.WARNING,
            "The active editor tab is not on a Java file. Please open another tab.");
      } else {
        // The editor does not work on a file (plugin.xml currently does not block this path).
        StatusLogger.logWarning(
            "The active editor tab does not work on a file. Please open another tab.");
      }
    } else if (activePart instanceof IViewPart) {
      ISelection sel = HandlerUtil.getCurrentSelection(event);
      // Only process single-file structured selection.
      if (sel != null && sel instanceof IStructuredSelection
          && ((IStructuredSelection) sel).size() == 1) {
        Object firstElement = ((IStructuredSelection) sel).getFirstElement();
        createUnitTestCaseOrError(firstElement, Status.ERROR, "Non-Java file is being selected.");
      }
    }
    return null;
  }

  private void createUnitTestCaseOrError(Object input, int severity, String errorMsg)
      throws ExecutionException {
    // Is the input a Java file?
    if (input instanceof ICompilationUnit) {
      // Yes. Generate the unit test cases within it.
      try {
        createUnitTestCase((ICompilationUnit) input);
      } catch (JavaModelException ex) {
        throw new ExecutionException(ex.getMessage(), ex);
      }
    } else {
      // No. Log the message.
      StatusLogger.log(severity, errorMsg);
    }
  }

  /**
   * Create the test case method and add required import if necessary
   * 
   * @param javaFile The input Java source file (.java)
   * @throws JavaModelException
   */
  private void createUnitTestCase(ICompilationUnit javaFile) throws JavaModelException {
    // Create import.
    javaFile.createImport("org.junit.*", null /* don't care about the position of the import */,
        null /* don't track the progress of creating import */);

    // Create the test case method in the public class of this Java file.
    // Scan through all classes and find the public class.
    for (IType type : javaFile.getTypes()) {
      if (Flags.isPublic(type.getFlags())) {
        // Check whether methods of the same signature exist.
        if (!type.getMethod("foo", new String[] {}).exists()) {
          String methodStr = "public void foo() {assert (true);}";
          type.createMethod(methodStr, /* the added method */
              null /* don't care where the method is added */,
              false /* don't replace user's method by our method */,
              null /* don't monitor the generation process*/);
        }
        // A Java file has at most public class.
        break;
      }
    }
  }
}
