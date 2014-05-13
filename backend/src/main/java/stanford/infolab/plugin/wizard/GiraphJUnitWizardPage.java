/*******************************************************************************
 * Copyright (c) 2000, 2012 IBM Corporation and others. All rights reserved. This program and the
 * accompanying materials are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: IBM Corporation - initial API and implementation Sebastian Davids, sdavids@gmx.de -
 * bug 38507 Sebastian Davids, sdavids@gmx.de - 113998 [JUnit] New Test Case Wizard: Class Under
 * Test Dialog -- allow Enums Kris De Volder <kris.de.volder@gmail.com> - Allow changing the default
 * superclass in NewTestCaseWizardPageOne - https://bugs.eclipse.org/312204
 *******************************************************************************/
package stanford.infolab.plugin.wizard;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.velocity.exception.VelocityException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jdt.core.IJavaElement;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.IPackageFragment;
import org.eclipse.jdt.core.IType;
import org.eclipse.jdt.core.JavaModelException;
import org.eclipse.jdt.ui.wizards.NewTypeWizardPage;

import stanford.infolab.debugger.utils.GiraphScenarioWrapper;
import stanford.infolab.plugin.Activator;
import stanford.infolab.plugin.mock.TestCaseGenerator;

/**
 * The class <code>NewTestCaseWizardPageOne</code> contains controls and validation routines for the
 * first page of the 'New JUnit TestCase Wizard'.
 * <p>
 * Clients can use the page as-is and add it to their own wizard, or extend it to modify validation
 * or add and remove controls.
 * </p>
 * 
 * @since 3.1
 */
public class GiraphJUnitWizardPage extends NewTypeWizardPage {

  private final static String PAGE_NAME = "GiraphJUnitWizardPage"; //$NON-NLS-1$

  /** Field ID of the scenario file field. */
  public final static String SCENARIO_FILE = "Scenario File"; //$NON-NLS-1$

  private final static String TITLE = "New Giraph Test Case"; //$NON-NLS-1$
  private final static String DESC = ""; //$NON-NLS-1$

  private String fScenarioFileName; // model
  @SuppressWarnings("rawtypes")
  private GiraphScenarioWrapper fScenario; // resolved model, can be null
  private Text fScenarioFileControl;
  private Button fScenarioFileButton;
  private IStatus fScenarioFileStatus = Status.CANCEL_STATUS;

  public GiraphJUnitWizardPage() {
    super(true, PAGE_NAME);

    setTitle(TITLE);
    setDescription(DESC);

    enableCommentControl(true);

    fScenarioFileName = ""; //$NON-NLS-1$
  }

  /**
   * Initialized the page with the current selection. The selection is used to specify the default
   * values for the new file's package. 
   * @param selection The selection
   */
  public void init(IStructuredSelection selection) {
    IJavaElement element = getInitialJavaElement(selection);
    initContainerPage(element);
    initTypePage(element);
    updateStatus(getStatusList());
  }

  /**
   * A generic method to handle modifications at any fields.
   * @see org.eclipse.jdt.ui.wizards.NewContainerWizardPage#handleFieldChanged(String)
   */
  @Override
  protected void handleFieldChanged(String fieldName) {
    super.handleFieldChanged(fieldName);
    updateStatus(getStatusList());
  }

  /**
   * Returns all status to be consider for the validation. Can only proceed when all statuses are
   * ok. Clients can override.
   * @return The list of status to consider for the validation.
   */
  protected IStatus[] getStatusList() {
    return new IStatus[] {fPackageStatus, fTypeNameStatus, fSuperClassStatus, fScenarioFileStatus};
  }


  /**
   * Create the control (and the panels) for the page's UI.
   * @see org.eclipse.jface.dialogs.IDialogPage#createControl(org.eclipse.swt.widgets.Composite)
   */
  public void createControl(Composite parent) {
    initializeDialogUnits(parent);

    Composite composite = new Composite(parent, SWT.NONE);

    int nColumns = 4;

    GridLayout layout = new GridLayout();
    layout.numColumns = nColumns;
    composite.setLayout(layout);
    createContainerControls(composite, nColumns);
    createPackageControls(composite, nColumns);
    createSeparator(composite, nColumns);
    createTypeNameControls(composite, nColumns);
    createSuperClassControls(composite, nColumns);
    createSeparator(composite, nColumns);
    createScenarioFileControls(composite, nColumns);

    setControl(composite);

    Dialog.applyDialogFont(composite);
    // PlatformUI.getWorkbench().getHelpSystem()
    // .setHelp(composite, IJUnitHelpContextIds.NEW_TESTCASE_WIZARD_PAGE);

    setFocus();
  }

  /**
   * Creates the controls for the 'Scenario file' field. Expects a <code>GridLayout</code> with at
   * least 3 columns.
   * 
   * @param composite the parent composite
   * @param nColumns number of columns to span
   */
  protected void createScenarioFileControls(Composite composite, int nColumns) {
    Label scenarioFileLabel = new Label(composite, SWT.LEFT | SWT.WRAP);
    scenarioFileLabel.setFont(composite.getFont());
    scenarioFileLabel.setText("Scenario file:");
    scenarioFileLabel.setLayoutData(new GridData());

    fScenarioFileControl = new Text(composite, SWT.SINGLE | SWT.BORDER);
    fScenarioFileControl.setEnabled(true);
    fScenarioFileControl.setFont(composite.getFont());
    fScenarioFileControl.setText(fScenarioFileName);
    GridData gd = new GridData();
    gd.horizontalAlignment = GridData.FILL;
    gd.grabExcessHorizontalSpace = true;
    // the 2 columns are for label and button
    gd.horizontalSpan = nColumns - 2;
    fScenarioFileControl.setLayoutData(gd);

    fScenarioFileButton = new Button(composite, SWT.PUSH);
    fScenarioFileButton.setText("Browse...");
    fScenarioFileButton.setEnabled(getPackageFragmentRoot() != null);
    fScenarioFileButton.addSelectionListener(new SelectionListener() {
      public void widgetDefaultSelected(SelectionEvent e) {
        scenarioFileButtonPressed();
      }

      public void widgetSelected(SelectionEvent e) {
        scenarioFileButtonPressed();
      }
    });
    gd = new GridData();
    gd.horizontalAlignment = GridData.FILL;
    gd.grabExcessHorizontalSpace = false;
    gd.horizontalSpan = 1;
    // gd.widthHint = LayoutUtil.getButtonWidthHint(fScenarioFileButton);
    fScenarioFileButton.setLayoutData(gd);
  }

  /**
   * Called when the button to select the scenario file is pressed.
   */
  private void scenarioFileButtonPressed() {
    String scenarioFileName = chooseScenarioFile();
    if (scenarioFileName != null) {
      setScenarioFileName(scenarioFileName);
    }
  }

  /**
   * Open a {@link FileDialog} for the user to choose a scenario file.
   * @return The new file's name.
   */
  private String chooseScenarioFile() {
    FileDialog dialog = new FileDialog(getShell(), SWT.OPEN);
    // the extension filter is .tr for now (i.e. only .tr files are displayed and selectable)
    dialog.setFilterExtensions(new String[] {"*.tr"});
    dialog.setFilterNames(new String[] {"Scenario files (.tr)"});
    String fileName = dialog.open(); // can be null
    return fileName;
  }

  protected void setScenarioFileName(String scenarioFileName) {
    if (fScenarioFileControl != null && !fScenarioFileControl.isDisposed()) {
      fScenarioFileControl.setText(scenarioFileName);
    }
    fScenarioFileName = scenarioFileName;
    fScenarioFileStatus = scenarioFileChanged();
    handleFieldChanged(SCENARIO_FILE);
  }

  /**
   * Called when the scenario filename changes. Load and validate the new scenario file.
   * @return
   */
  @SuppressWarnings("rawtypes")
  protected IStatus scenarioFileChanged() {
    fScenario = new GiraphScenarioWrapper();
    try {
      fScenario.load(fScenarioFileName);
    } catch (ClassNotFoundException|IOException|InstantiationException|IllegalAccessException e) {
      e.printStackTrace();
      return new Status(ERROR, Activator.PLUGIN_ID, "Can't load the scenario file!", e);
    }
    return Status.OK_STATUS;
  }

  @SuppressWarnings("rawtypes")
  public GiraphScenarioWrapper getScenario() {
    return fScenario;
  }

  /**
   * Called by {@link NewTypeWizardPage#createType(IProgressMonitor)} to create the type members
   * for the new types.
   * @see org.eclipse.jdt.ui.wizards.NewTypeWizardPage#createTypeMembers(org.eclipse.jdt.core.IType,
   *      org.eclipse.jdt.ui.wizards.NewTypeWizardPage.ImportsManager,
   *      org.eclipse.core.runtime.IProgressMonitor)
   */
  @SuppressWarnings("rawtypes")
  @Override
  protected void createTypeMembers(IType type, ImportsManager imports, IProgressMonitor monitor)
      throws CoreException {
    GiraphScenarioWrapper scenario = getScenario();
    // add imports if not exist
    imports.addStaticImport("org.junit.Assert", "*", false);
    imports.addStaticImport("org.mockito.Mockito", "*", false);
    imports.addImport("java.io.IOException");
    imports.addImport("java.util.ArrayList");
    imports.addImport("org.apache.giraph.conf.GiraphConfiguration");
    imports.addImport("org.apache.giraph.conf.ImmutableClassesGiraphConfiguration");
    imports.addImport("org.apache.giraph.edge.ReusableEdge");
    imports.addImport("org.apache.giraph.graph.GraphState");
    imports.addImport("org.apache.giraph.graph.Vertex");
    imports.addImport("org.apache.giraph.worker.WorkerAggregatorUsage");
    imports.addImport("org.apache.giraph.utils.MockUtils.MockedEnvironment");

    HashSet<Class> usedTypes = new LinkedHashSet<>(6);
    usedTypes.add(scenario.getClassUnderTest());
    usedTypes.add(scenario.getVertexIdClass());
    usedTypes.add(scenario.getVertexValueClass());
    usedTypes.add(scenario.getEdgeValueClass());
    usedTypes.add(scenario.getIncomingMessageClass());
    usedTypes.add(scenario.getOutgoingMessageClass());
    for (Class usedType : usedTypes) {
      imports.addImport(usedType.getName());
    }

    // add methods
    createSetUp(type, imports);
    createTestCompute(type, imports);
  }

  private void createSetUp(IType type, ImportsManager imports) throws CoreException {
    @SuppressWarnings("rawtypes")
    GiraphScenarioWrapper scenario = getScenario();

    imports.addImport("org.junit.Before");
    TestCaseGenerator generator = new TestCaseGenerator();
    String content;
    try {
      // for all type.createField(), we don't specify where the fields are (the first null),
      // don't monitor the process (the second null) and force the change in the fields if the
      // fields already exist (the true flag)
      type.createField(generator.generateClassUnderTestField(scenario), null, true, null);
      type.createField(generator.generateConfField(scenario), null, true, null);
      type.createField(generator.generateMockEnvField(scenario), null, true, null);

      content = generator.generateSetUp(scenario);
      type.createMethod(content, null, true, null);
    } catch (VelocityException | IOException e) {
      e.printStackTrace();
    }
  }

  private void createTestCompute(IType type, ImportsManager imports) throws CoreException {
    @SuppressWarnings("rawtypes")
    GiraphScenarioWrapper scenario = getScenario();

    imports.addImport("org.junit.Test");
    TestCaseGenerator generator = new TestCaseGenerator();
    String content;
    try {
      content = generator.generateTestCompute(scenario);
      type.createMethod(content, null, true, null);
      
      if (!generator.getUnsolvedWritableSet().isEmpty()) {
        imports.addImport("org.apache.giraph.utils.WritableUtils");
        for (Class unsolvedWritableClass : generator.getUnsolvedWritableSet()) {
          content = generator.generateReadWritableFromString(unsolvedWritableClass.getSimpleName());
          type.createMethod(content, null, true, null);
        }
      }
    } catch (VelocityException | IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Called when the superclass textbox is changed. Validate the new superclass. Ensure that the 
   * superclass is in the class path.  
   * @see org.eclipse.jdt.ui.wizards.NewTypeWizardPage#superClassChanged()
   */
  @Override
  protected IStatus superClassChanged() {
    IStatus stat = super.superClassChanged();
    if (stat.getSeverity() != IStatus.OK)
      return stat;
    String superClassName = getSuperClass();
    if (superClassName == null || superClassName.trim().equals("")
        || superClassName.equals("java.lang.Object")) {
      return Status.OK_STATUS;
    }
    if (getPackageFragmentRoot() != null) {
      try {
        IType type =
            resolveClassNameToType(getPackageFragmentRoot().getJavaProject(), getPackageFragment(),
                superClassName);
        if (type == null) {
          return new Status(IStatus.WARNING, Activator.PLUGIN_ID, "Superclass not exists!");
        }
      } catch (JavaModelException e) {
        return new Status(IStatus.ERROR, Activator.PLUGIN_ID, e.getMessage(), e);
      }
    }
    return Status.OK_STATUS;
  }

  /**
   * Always return false since this is the last page.
   * @see org.eclipse.jface.wizard.IWizardPage#canFlipToNextPage()
   */
  @Override
  public boolean canFlipToNextPage() {
    return false;
  }

  /**
   * Find the corresponding {@link IType} for the given class's name.
   */
  private IType resolveClassNameToType(IJavaProject jproject, IPackageFragment pack,
      String classToTestName) throws JavaModelException {
    if (!jproject.exists()) {
      return null;
    }

    IType type = jproject.findType(classToTestName);

    // search in current package
    if (type == null && pack != null && !pack.isDefaultPackage()) {
      type = jproject.findType(pack.getElementName(), classToTestName);
    }

    // search in java.lang
    if (type == null) {
      type = jproject.findType("java.lang", classToTestName); //$NON-NLS-1$
    }
    return type;
  }
}
