package com.netflix.metacat.connector.polaris.common;

import com.google.common.base.Throwables;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.monitoring.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.Ordered;
import org.springframework.retry.RetryException;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.SQLException;

/**
 * Aspect for client-side transaction retries.
 */
@Aspect
@Slf4j
public class TransactionRetryAspect implements Ordered {
    private static final String SQLSTATE_RETRY_TRANSACTION = "40001";
    private final RetryTemplate retryTemplate;
    private final ConnectorContext connectorContext;

    /**
     * Constructor.
     *
     * @param retryTemplate retry template.
     * @param connectorContext the connector context.
     */
    public TransactionRetryAspect(final RetryTemplate retryTemplate,
                                  final ConnectorContext connectorContext) {
        this.retryTemplate = retryTemplate;
        this.connectorContext = connectorContext;
    }

    /**
     * Pointcut for transactional methods in Polaris persistence classes.
     *
     * @param pjp   joint point
     * @return data results
     * @throws Exception data exception
     */
    @Around(value = "@annotation(org.springframework.transaction.annotation.Transactional)"
            + "&& within(com.netflix.metacat.connector.polaris.store..*)")
    public Object retry(final ProceedingJoinPoint pjp) throws Exception {
        return retryOnError(pjp);
    }

    private Object retryOnError(final ProceedingJoinPoint pjp) throws Exception {
        return retryTemplate.<Object, Exception>execute(context -> {
            try {
                return pjp.proceed();
            } catch (Throwable t) {
                if (!TransactionSynchronizationManager.isActualTransactionActive() && isRetryError(t)) {
                    log.warn("Transaction failed with retry error: {}", t.getMessage());
                    connectorContext.getRegistry().counter(
                            Metrics.CounterTransactionRetryFailure.getMetricName()).increment();
                    throw new RetryException("TransactionRetryError", t);
                }
                throw new RuntimeException(t);
            }
        });
    }

    private boolean isRetryError(final Throwable t) {
        for (Throwable ex : Throwables.getCausalChain(t)) {
            if (ex instanceof SQLException && SQLSTATE_RETRY_TRANSACTION.equals(((SQLException) ex).getSQLState())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getOrder() {
        return 99;
    }
}

