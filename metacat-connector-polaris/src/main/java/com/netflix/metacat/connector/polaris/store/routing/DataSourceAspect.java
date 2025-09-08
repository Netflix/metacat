package com.netflix.metacat.connector.polaris.store.routing;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * DataSourceAspect for controlling the routing of query.
 */
@Aspect
@Component
public class DataSourceAspect {
    /**
     * Sets the datasource context before executing a method annotated with {@link Transactional}.
     * If the transaction is marked as read-only, the context is set to use the replica (read) datasource.
     * Otherwise, it is set to use the primary (write) datasource.
     *
     * @param transactional the {@link Transactional} annotation of the target method
     */
    @Before("@annotation(transactional)")
    public void setDataSourceType(final Transactional transactional) {
        if (transactional.readOnly()) {
            DataSourceContextHolder.setContext(DataSourceEnum.REPLICA);
        } else {
            DataSourceContextHolder.setContext(DataSourceEnum.PRIMARY);
        }
    }

    /**
     * Clears the datasource context after the execution of any method in the target package.
     * This ensures that the datasource context does not leak between different method invocations or threads.
     */
    @After("execution(* com.netflix.metacat.connector.polaris..*.*(..))")
    public void clearDataSourceType() {
        DataSourceContextHolder.clearContext();
    }
}
