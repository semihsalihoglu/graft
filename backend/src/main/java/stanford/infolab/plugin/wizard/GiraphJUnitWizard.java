package stanford.infolab.plugin.wizard;

import org.eclipse.jdt.core.IType;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.INewWizard;
import org.eclipse.ui.IWorkbench;
import org.eclipse.jface.operation.*;

import java.lang.reflect.InvocationTargetException;

import org.eclipse.core.resources.*;
import org.eclipse.ui.*;
import org.eclipse.ui.actions.WorkspaceModifyDelegatingOperation;
import org.eclipse.ui.ide.IDE;
import org.eclipse.ui.wizards.newresource.BasicNewResourceWizard;

import stanford.infolab.plugin.Activator;

public class GiraphJUnitWizard extends Wizard implements INewWizard {

  // private GiraphJUnitWizardPage page;
  private GiraphJUnitWizardPage page;
  private IWorkbench workbench;
  private IStructuredSelection selection;

  /**
   * Constructor for GiraphJUnitWizard.
   */
  public GiraphJUnitWizard() {
    super();
    setNeedsProgressMonitor(true);
    setDefaultPageImageDescriptor(Activator.getImageDescriptor("wizban/newtest_wiz.png")); //$NON-NLS-1$
  }

  /**
   * Adding the page to the wizard.
   * 
   * @see IWizard#addPage(org.eclipse.jface.wizard.IWizardPage)
   */
  @Override
  public void addPages() {
    // page = new GiraphJUnitWizardPage(selection);
    // addPage(page);
    page = new GiraphJUnitWizardPage();
    addPage(page);
    page.init(selection);
  }
  
  /**
   * @see Wizard#canFinish()
   */
  @Override
  public boolean canFinish() {
    return page.getPackageFragment() != null && page.getScenario() != null;
  }

  /**
   * @see IWizard#performFinish
   */
  @Override
  public boolean performFinish() {
    IRunnableWithProgress runnable = page.getRunnable();
    if (finishPage(runnable)) {
      IType newClass = page.getCreatedType();
      IResource resource = newClass.getCompilationUnit().getResource();
      if (resource != null) {
        selectAndReveal(resource);
        openResource(resource);
      }
      return true;
    }
    return false;
  }

  /*
   * Run a runnable
   */
  protected boolean finishPage(IRunnableWithProgress runnable) {
    IRunnableWithProgress op = new WorkspaceModifyDelegatingOperation(runnable);
    try {
      PlatformUI.getWorkbench().getProgressService()
          .runInUI(getContainer(), op, ResourcesPlugin.getWorkspace().getRoot());

    } catch (InvocationTargetException e) {
      return false;
    } catch (InterruptedException e) {
      return false;
    }
    return true;
  }

  protected void selectAndReveal(IResource newResource) {
    BasicNewResourceWizard.selectAndReveal(newResource, workbench.getActiveWorkbenchWindow());
  }

  protected void openResource(final IResource resource) {
    if (resource.getType() == IResource.FILE) {
      final IWorkbenchPage activePage = workbench.getActiveWorkbenchWindow().getActivePage();
      if (activePage != null) {
        final Display display = Display.getDefault();
        if (display != null) {
          display.asyncExec(new Runnable() {
            public void run() {
              try {
                IDE.openEditor(activePage, (IFile) resource, true);
              } catch (PartInitException e) {
                e.printStackTrace();
              }
            }
          });
        }
      }
    }
  }

  /**
   * We will accept the selection in the workbench to see if we can initialize from it.
   * 
   * @see IWorkbenchWizard#init(IWorkbench, IStructuredSelection)
   */
  public void init(IWorkbench workbench, IStructuredSelection selection) {
    this.workbench = workbench;
    this.selection = selection;
  }
}
