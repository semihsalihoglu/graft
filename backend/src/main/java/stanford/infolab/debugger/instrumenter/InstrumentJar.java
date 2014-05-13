package stanford.infolab.debugger.instrumenter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

/**
 * InstrumentJar class for instrumenting user's ordinary Giraph Computation
 * class with the necessary changes for debugging.
 * 
 * @author netj
 */
public class InstrumentJar {

	private static final String DEBUG_CLASS_NAME_SUFFIX = System.getProperty(
			"giraph.debugger.classNameSuffix", "Instrumented");
	private static final String INTERCEPTOR_CLASS_NAME_SUFFIX = System
			.getProperty("giraph.debugger.interceptorSuffix", "Interceptor");

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.err
					.println("Usage: java ... InstrumentJar GIRAPH_COMPUTATION_CLASS_NAME  [OUTPUT_DIR]");
			System.exit(1);
		}

		String userComputationClassName = args[0];
		String outputDir = (args.length > 1 ? args[1] : null);

		try {
			// Load the involved classes with Javassist
			ClassPool classPool = ClassPool.getDefault();
			String interceptingClassName = userComputationClassName
					+ INTERCEPTOR_CLASS_NAME_SUFFIX;
			CtClass interceptingClass = classPool.getAndRename(
					AbstractInterceptingComputation.class.getCanonicalName(),
					interceptingClassName);
			String debugComputationClassName = userComputationClassName
					+ DEBUG_CLASS_NAME_SUFFIX;
			CtClass debugComputationClass = classPool.getAndRename(
					userComputationClassName, debugComputationClassName);

			// TODO Instrumentation cannot be done just by injecting a superclass.
			//      We probably need two classes: one at the bottom (subclass) and
			//      another at the top (superclass).
			// 1. To intercept compute():
			//    We need to extend user's computation class pass that as the
			//    computation class to Giraph. The new subclass will override
			//    compute(), so we may have to remove the "final" marker on user's
			//    compute() method.
			// 2. To intercept other Giraph API calls by user's computation class:
			//    We need to inject a superclass at the highest level of the
			//    inheritance hierarchy, i.e., make AbstractInterceptingComputation
			//    become a superclass of the class that directly extends AbstractComputation.

			// 1. Rename compute() to computeFurther()
			CtMethod computeMethod = debugComputationClass.getMethod("compute",
					"(Lorg/apache/giraph/graph/Vertex;Ljava/lang/Iterable;)V");
			computeMethod.setName("computeFurther");

			// 2. Let the class extend AbstractInterceptingComputation instead
			CtClass oldSuperClass = debugComputationClass.getSuperclass();
			debugComputationClass.setSuperclass(interceptingClass);
			// We should also modify AbstractInterceptingComputation to
			// extend the user's original super class instead.
			interceptingClass.setSuperclass(oldSuperClass);

			// Write the modified classes out so that a new jar can be created
			String jarRoot = outputDir != null ? outputDir : Files
					.createTempDirectory("InstrumentJar", new FileAttribute[0])
					.toString();
			interceptingClass.writeFile(jarRoot);
			debugComputationClass.writeFile(jarRoot);

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
}
