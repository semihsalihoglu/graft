package stanford.infolab.debugger.instrumenter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for the {@link BottomInterceptingComputation} class to communicate
 * which methods are to be intercepted and checked by the instrumenter.
 * 
 * @author netj
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface Intercept {
}
