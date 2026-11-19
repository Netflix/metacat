/*
 *       Copyright 2026 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.metadata.mysql;

import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.NameMatchMethodPointcutAdvisor;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.interceptor.RetryInterceptorBuilder;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;

/**
 * Retries user metadata reads that fail on a broken database connection.
 * <p>
 * A pooled connection can break underneath the pool, e.g. when the proxy it runs through goes away, and the pool
 * keeps handing it out until the connection is next validated. Reads are idempotent, so they are retried on a new
 * connection. The retry wraps the transactional service, so each attempt runs in its own transaction; a read that
 * joins a caller's transaction is not retried, since it would get the same connection back.
 */
public final class UserMetadataReadRetry {
    static final int MAX_ATTEMPTS = 3;
    private static final String[] READ_METHODS = {"get*", "populateMetadata", "search*"};

    private UserMetadataReadRetry() {
    }

    /**
     * Wraps the service so its reads are retried on connection failures.
     *
     * @param service transactional user metadata service
     * @return the service with retried reads
     */
    public static UserMetadataService wrap(final UserMetadataService service) {
        // backs off past the pool's default validation interval of 3 seconds
        final ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2);
        return wrap(service, backOff);
    }

    static UserMetadataService wrap(final UserMetadataService service, final BackOffPolicy backOff) {
        final NameMatchMethodPointcutAdvisor advisor = new NameMatchMethodPointcutAdvisor(
            RetryInterceptorBuilder.stateless()
                .retryPolicy(new ConnectionFailureRetryPolicy())
                .backOffPolicy(backOff)
                .build());
        advisor.setMappedNames(READ_METHODS);

        final ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(service);
        proxyFactory.setInterfaces(UserMetadataService.class);
        proxyFactory.addAdvisor(advisor);
        proxyFactory.setFrozen(true);
        return (UserMetadataService) proxyFactory.getProxy(UserMetadataReadRetry.class.getClassLoader());
    }

    private static final class ConnectionFailureRetryPolicy extends SimpleRetryPolicy {
        private static final long serialVersionUID = 1L;

        ConnectionFailureRetryPolicy() {
            super(MAX_ATTEMPTS,
                Map.of(
                    SQLRecoverableException.class, true,
                    SQLNonTransientConnectionException.class, true,
                    SQLTransientConnectionException.class, true),
                true);
        }

        @Override
        public boolean canRetry(final RetryContext context) {
            return context.getLastThrowable() == null
                || !TransactionSynchronizationManager.isActualTransactionActive() && super.canRetry(context);
        }
    }
}
