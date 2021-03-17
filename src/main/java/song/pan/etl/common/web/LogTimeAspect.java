package song.pan.etl.common.web;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Aspect
@Component
@Slf4j
public class LogTimeAspect {

    @Pointcut("execution(public * song.pan.etl.controller..*.*(..))")
    public void controller() {

    }

    @Around("controller()")
    public Object logTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long begin = System.currentTimeMillis();
        try {
            return joinPoint.proceed();
        } catch (Throwable e) {
            throw e;
        } finally {
            MethodSignature signature = (MethodSignature) joinPoint.getSignature();
            Method method = joinPoint.getTarget().getClass().getMethod(signature.getName(), signature.getParameterTypes());
            log.info("[Time] {}.{} [{} ms]", method.getDeclaringClass().getName(), method.getName(), System.currentTimeMillis() - begin);
        }
    }

}
