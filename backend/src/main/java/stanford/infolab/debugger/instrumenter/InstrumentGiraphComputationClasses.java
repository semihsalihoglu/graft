package stanford.infolab.debugger.instrumenter;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.Collection;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.bytecode.Descriptor;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

/**
 * The main class for that instruments user's ordinary Giraph Computation class
 * with necessary changes for debugging.
 * 
 * @author netj
 */
public class InstrumentGiraphComputationClasses {

	private static Logger LOG = Logger
			.getLogger(InstrumentGiraphComputationClasses.class);

	private static final String INTERCEPTOR_CLASS_NAME_SUFFIX = System
			.getProperty("giraph.debugger.classNameSuffix", "Debug");
	private static final String tmpDirNamePrefix = InstrumentGiraphComputationClasses.class
			.getSimpleName();

	public static void main(String[] args) throws IOException, Exception {
		if (args.length < 1) {
			System.err.println("Usage: java ... " + tmpDirNamePrefix
					+ " GIRAPH_COMPUTATION_CLASS_NAME  [OUTPUT_DIR]");
			System.exit(1);
		}

		String userComputationClassName = args[0];
		String outputDir = (args.length > 1 ? args[1] : null);

		try {
			// Load the involved classes with Javassist
			LOG.info("Looking for classes...");
			String interceptingClassName = userComputationClassName
					+ INTERCEPTOR_CLASS_NAME_SUFFIX;
			ClassPool classPool = ClassPool.getDefault();
			CtClass userComputationClass = classPool
					.get(userComputationClassName);
			// We need two classes: one at the bottom (subclass) and
			// another at the top (superclass).
			CtClass topClass = classPool
					.get(AbstractInterceptingComputation.class
							.getCanonicalName());
			CtClass bottomClass = classPool.getAndRename(
					BottomInterceptingComputation.class.getCanonicalName(),
					interceptingClassName);

			LOG.info("  user's Computation class (userComputationClass):\n"
					+ getGenericsName(userComputationClass));
			LOG.info("  class to instrument at top (topClass):\n"
					+ getGenericsName(topClass));
			LOG.info("  class to instrument at bottom (bottomClass):\n"
					+ getGenericsName(bottomClass));

			Collection<CtClass> classesModified = Sets.newHashSet();
			// 1. To intercept other Giraph API calls by user's computation
			// class:
			// We need to inject a superclass at the highest level of the
			// inheritance hierarchy, i.e., make the top class become a
			// superclass of the class that directly extends
			// AbstractComputation.
			// 1-a. Find the user's base class that extends the top class'
			// superclass.
			LOG.info("Looking for user's top class that extends "
					+ getGenericsName(topClass.getSuperclass()));
			CtClass userTopClass = userComputationClass;
			while (!userTopClass.equals(Object.class)
					&& !userTopClass.getSuperclass().equals(
							topClass.getSuperclass())) {
				userTopClass = userTopClass.getSuperclass();
			}
			if (userTopClass.equals(Object.class)) {
				throw new NotFoundException(userComputationClass.getName()
						+ " must extend " + topClass.getSuperclass().getName());
			}
			LOG.info("  class to inject topClass on top of (userTopClass):\n"
					+ getGenericsName(userTopClass));
			// 1-b. Mark user's class as abstract and erase any final modifier.
			LOG.info("Marking userComputationClass as abstract and non-final...");
			{
				int mod = userComputationClass.getModifiers();
				mod |= Modifier.ABSTRACT;
				mod &= ~Modifier.FINAL;
				userComputationClass.setModifiers(mod);
				classesModified.add(userComputationClass);
			}
			// 1-c. Inject the top class by setting it as the superclass of
			// user's class that extends its superclass (AbstractComputation).
			LOG.info("Injecting topClass on top of userTopClass...");
			userTopClass.setSuperclass(topClass);
			userTopClass.replaceClassName(topClass.getSuperclass().getName(),
					topClass.getName());
			// XXX Unless we take care of generic signature as well,
			// GiraphConfigurationValidator will complain.
			String jvmNameForTopClassSuperclass = Descriptor.of(
					topClass.getSuperclass()).replaceAll(";$", "");
			String jvmNameForTopClass = Descriptor.of(topClass).replaceAll(
					";$", "");
			String sig = userTopClass.getGenericSignature().replace(
					jvmNameForTopClassSuperclass, jvmNameForTopClass);
			userTopClass.setGenericSignature(sig);
			classesModified.add(userTopClass);
			// 1-d. Then, make the bottomClass extend user's computation, taking
			// care of generics signature as well.
			LOG.info("Attaching bottomClass beneath userComputationClass...");
			bottomClass.replaceClassName(
					UserComputation.class.getCanonicalName(),
					userComputationClass.getName());
			bottomClass.setSuperclass(userComputationClass);
			bottomClass
					.setGenericSignature(Descriptor.of(userComputationClass));
			classesModified.add(bottomClass);

			// 2. To intercept compute() and other calls that originate from
			// Giraph:
			// We need to extend user's computation class pass that as the
			// computation class to Giraph. The new subclass will override
			// compute(), so we may have to remove the "final" marker on user's
			// compute() method.
			// 2-a. Find all methods that we override in bottomClass.
			LOG.info("For each method to intercept,"
					+ " changing Generics signature of bottomClass, and"
					+ " erasing final modifier...");
			CtMethod[] declaredMethods = bottomClass.getDeclaredMethods();
			for (CtMethod overridingMethod : declaredMethods) {
				if (!overridingMethod.hasAnnotation(Intercept.class))
					continue;
				// 2-b. Copy generics signature to bottomClass.
				CtMethod userMethod = userComputationClass.getMethod(
						overridingMethod.getName(),
						overridingMethod.getSignature());
				LOG.debug(" from: " + overridingMethod.getName()
						+ overridingMethod.getGenericSignature());
				LOG.debug("   to: " + userMethod.getName()
						+ userMethod.getGenericSignature());
				if (overridingMethod.getGenericSignature() != null)
					overridingMethod.setGenericSignature(userMethod
							.getGenericSignature());
				// 2-c. Remove final marks from them.
				int mod = userMethod.getModifiers();
				if ((mod & Modifier.FINAL) == 0)
					continue;
				mod &= ~Modifier.FINAL;
				userMethod.setModifiers(mod);
				LOG.debug(" erasing final modifier from "
						+ userMethod.getName() + "() of "
						+ userMethod.getDeclaringClass());
				// 2-d. Remember them for later.
				classesModified.add(userMethod.getDeclaringClass());
			}

			// 3. Finally, write the modified classes so that a new jar can be
			// created or an existing one can be updated.
			String jarRoot = outputDir != null ? outputDir
					: Files.createTempDirectory(tmpDirNamePrefix,
							new FileAttribute[0]).toString();
			LOG.info("Writing instrumented classes to " + jarRoot);
			LOG.debug("            topClass=\n" + getGenericsName(topClass)
					+ "\n" + topClass);
			LOG.debug("        userTopClass=\n" + getGenericsName(userTopClass)
					+ "\n" + userTopClass);
			LOG.debug("userComputationClass=\n"
					+ getGenericsName(userComputationClass) + "\n"
					+ userComputationClass);
			LOG.debug("         bottomClass=\n" + getGenericsName(bottomClass)
					+ "\n" + bottomClass);
			for (CtClass c : classesModified) {
				LOG.debug(" writing class " + c.getName());
				c.writeFile(jarRoot);
			}

			if (outputDir == null)
				// Show where we produced the instrumented .class files (unless
				// specified)
				System.out.println(jarRoot);
			System.exit(0);
		} catch (NotFoundException e) {
			System.err.println(userComputationClassName
					+ ": Giraph Computation class not found");
			System.exit(1);
		} catch (CannotCompileException e) {
			e.printStackTrace();
			System.err.println(userComputationClassName
					+ ": Cannot instrument the given Giraph Computation class");
			System.exit(2);
		} catch (IOException e) {
			e.printStackTrace();
			System.err
					.println(userComputationClassName
							+ ": Cannot write the instrumented Giraph Computation class");
			System.exit(4);
		}

	}

	protected static String getGenericsName(CtClass clazz) {
		return clazz.getName() + " (" + clazz.getGenericSignature() + ")";
	}
}
