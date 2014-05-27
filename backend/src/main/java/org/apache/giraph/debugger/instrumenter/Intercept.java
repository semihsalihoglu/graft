package org.apache.giraph.debugger.instrumenter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation normally for the {@link BottomInterceptingComputation} class to
 * communicate which methods are to be intercepted and checked by the
 * instrumenter.
 * 
 * @author netj
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface Intercept {
	/**
	 * The name to which this method should be renamed by the instrumenter. This
	 * is for telling the instrumenter to rename certain methods that are not
	 * normally overridable because of its final modifier.
	 */
	String renameTo() default "";

}
