/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.main.api;

import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.NotFoundException;
import com.netflix.metacat.common.server.connectors.exception.PartitionAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Request wrapper.
 *
 * @author amajumdar
 * @since 0.1.50
 */
@Slf4j
@Component
public final class RequestWrapper {
    private final Registry registry;
    //Metrics
    private final Id requestCounterId;
    private final Id requestFailureCounterId;
    private final Id requestTimerId;

    /**
     * Wrapper class for processing the request.
     *
     * @param registry registry
     */
    @Autowired
    public RequestWrapper(@NotNull @NonNull final Registry registry) {
        this.registry = registry;
        requestCounterId = registry.createId(Metrics.CounterRequestCount.getMetricName());
        requestFailureCounterId = registry.createId(Metrics.CounterRequestFailureCount.getMetricName());
        requestTimerId = registry.createId(Metrics.TimerRequest.getMetricName());
    }

    /**
     * Creates the qualified name.
     *
     * @param nameSupplier supplier
     * @return name
     */
    public QualifiedName qualifyName(final Supplier<QualifiedName> nameSupplier) {
        try {
            return nameSupplier.get();
        } catch (Exception e) {
            log.error("Invalid qualified name", e);
            throw new MetacatBadRequestException(e.getMessage());
        }
    }

    /**
     * Request wrapper to to process request.
     *
     * @param name                name
     * @param resourceRequestName request name
     * @param supplier            supplier
     * @param <R>                 response
     * @return response of supplier
     */
    public <R> R processRequest(
        final QualifiedName name,
        final String resourceRequestName,
        final Supplier<R> supplier) {
        final long start = registry.clock().wallTime();
        final Map<String, String> tags = new HashMap<>(name.parts());
        tags.put("request", resourceRequestName);
        registry.counter(requestCounterId.withTags(tags)).increment();

        try {
            log.info("### Calling method: {} for {}", resourceRequestName, name);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation");
        } catch (DatabaseAlreadyExistsException | TableAlreadyExistsException | PartitionAlreadyExistsException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatAlreadyExistsException(e.getMessage());
        } catch (NotFoundException | MetacatNotFoundException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatNotFoundException(
                String.format("Unable to locate for %s. Details: %s", name, e.getMessage()));
        } catch (InvalidMetaException | IllegalArgumentException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatBadRequestException(
                String.format("%s.%s", e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage()));
        } catch (ConnectorException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String.format("%s.%s -- %s failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            throw new MetacatException(message, e);
        } catch (UserMetadataServiceException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String.format("%s.%s -- %s usermetadata operation failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            throw new MetacatUserMetadataException(message);
        } catch (Exception e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String.format("%s.%s -- %s failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            throw new MetacatException(message, e);
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("### Time taken to complete {} for {} is {} ms", resourceRequestName, name, duration);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Simple request wrapper to process request.
     *
     * @param resourceRequestName request name
     * @param supplier            supplier
     * @param <R>                 response
     * @return response of the supplier
     */
    public <R> R processRequest(
        final String resourceRequestName,
        final Supplier<R> supplier) {
        final long start = registry.clock().wallTime();
        final Map<String, String> tags = Maps.newHashMap();
        tags.put("request", resourceRequestName);
        registry.counter(requestCounterId.withTags(tags)).increment();
        try {
            log.info("### Calling method: {}", resourceRequestName);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation");
        } catch (IllegalArgumentException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatBadRequestException(String.format("%s.%s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage()));
        } catch (Exception e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String
                .format("%s.%s -- %s failed.",
                    e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage(),
                    resourceRequestName);
            log.error(message, e);
            throw new MetacatException(message, e);
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("### Time taken to complete {} is {} ms", resourceRequestName,
                duration);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
        }
    }

    private void collectRequestExceptionMetrics(final Map<String, String> tags, final String exceptionName) {
        tags.put("exception", exceptionName);
        registry.counter(requestFailureCounterId.withTags(tags)).increment();
    }

}
