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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException;
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.common.server.api.traffic_control.RequestGateway;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.NotFoundException;
import com.netflix.metacat.common.server.connectors.exception.PartitionAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.AliasService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataServiceException;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.histogram.PercentileTimer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.validation.constraints.NotNull;
import java.util.Collections;
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
    private final Config config;
    private final AliasService aliasService;
    private final RequestGateway requestGateway;

    //Metrics
    private final Id requestCounterId;
    private final Id requestFailureCounterId;
    private final Id requestTimerId;
    private final Id requestPercentileTimerId;

    /**
     * Wrapper class for processing the request.
     *
     * @param registry registry
     * @param config Config
     * @param aliasService AliasService
     * @param requestGateway RequestGateway
     */
    @Autowired
    public RequestWrapper(@NotNull @NonNull final Registry registry,
                          @NotNull @NonNull final Config config,
                          @NotNull @NonNull final AliasService aliasService,
                          @NotNull @NonNull final RequestGateway requestGateway) {
        this.registry = registry;
        this.config = config;
        this.aliasService = aliasService;
        this.requestGateway = requestGateway;

        requestCounterId = registry.createId(Metrics.CounterRequestCount.getMetricName());
        requestFailureCounterId = registry.createId(Metrics.CounterRequestFailureCount.getMetricName());
        requestTimerId = registry.createId(Metrics.TimerRequest.getMetricName());
        requestPercentileTimerId = registry.createId(Metrics.PercentileTimerRequest.getMetricName());
    }

    /**
     * Creates the qualified name.
     *
     * @param nameSupplier supplier
     * @return name
     */
    public QualifiedName qualifyName(final Supplier<QualifiedName> nameSupplier) {
        try {
            final QualifiedName name = nameSupplier.get();
            if (config.isTableAliasEnabled() && name.getType() == QualifiedName.Type.TABLE) {
                return aliasService.getTableName(name);
            }
            return name;
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
        return processRequest(name, resourceRequestName, Collections.emptyMap(), supplier);
    }

    /**
     * Request wrapper to to process request.
     *
     * @param name                name
     * @param resourceRequestName request name
     * @param requestTags         tags that needs to be added to the registry
     * @param supplier            supplier
     * @param <R>                 response
     * @return response of supplier
     */
    public <R> R processRequest(
        final QualifiedName name,
        final String resourceRequestName,
        final Map<String, String> requestTags,
        final Supplier<R> supplier) {
        final long start = registry.clock().wallTime();
        final Map<String, String> tags = new HashMap<>(name.partsWithCatalogAndDatabase());
        final Map<String, String> percentileTags = Maps.newHashMap();

        if (requestTags != null) {
            tags.putAll(requestTags);
        }
        tags.put("request", resourceRequestName);
        tags.put("scheme", MetacatContextManager.getContext().getScheme());
        percentileTags.put("request", resourceRequestName);
        String clientAppName =  MetacatContextManager.getContext().getClientAppName();
        if (clientAppName == null) {
            clientAppName = "UNKNOWN";
        }
        tags.put("caller", clientAppName);
        registry.counter(requestCounterId.withTags(tags)).increment();

        try {
            requestGateway.validateRequest(resourceRequestName, name);
            MetacatContextManager.getContext().setRequestName(resourceRequestName);
            log.info("### Calling method: {} for {}", resourceRequestName, name);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation. " + e.getMessage());
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
        } catch (TablePreconditionFailedException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            log.error(e.getMessage(), e);
            throw new MetacatPreconditionFailedException(
                String.format("%s.%s", e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage()));
        } catch (ConnectorException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String.format("%s.%s -- %s failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            for (Throwable ex : Throwables.getCausalChain(e)) {
                if (ex.getMessage().contains("too many connections")
                    || ex.getMessage().contains("Timeout: Pool empty")) {
                    throw new MetacatTooManyRequestsException(ex.getMessage());
                }
            }
            throw new MetacatException(message, e);
        } catch (UserMetadataServiceException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String.format("%s.%s -- %s usermetadata operation failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            throw new MetacatUserMetadataException(message);
        }  catch (Exception e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            final String message = String.format("%s.%s -- %s failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            if (e instanceof MetacatException) {
                throw e;
            } else {
                throw new MetacatException(message, e);
            }
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("### Time taken to complete {} for {} is {} ms", resourceRequestName, name, duration);
            tryAddTableTypeTag(tags, name);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
            PercentileTimer.get(this.registry, requestPercentileTimerId.withTags(percentileTags))
                .record(duration, TimeUnit.MILLISECONDS);
        }
    }

    private static void tryAddTableTypeTag(final Map<String, String> tags, final QualifiedName qualifiedName) {
        final MetacatRequestContext context = MetacatContextManager.getContext();
        final String tableType = context.getTableType(qualifiedName);
        if (!StringUtils.isBlank(tableType)) {
            tags.put("tableType", tableType.toLowerCase());
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
        final Map<String, String> percentileTags = Maps.newHashMap();
        tags.put("request", resourceRequestName);
        percentileTags.put("request", resourceRequestName);
        String clientAppName =  MetacatContextManager.getContext().getClientAppName();
        if (clientAppName == null) {
            clientAppName = "UNKNOWN";
        }
        tags.put("caller", clientAppName);
        registry.counter(requestCounterId.withTags(tags)).increment();
        try {
            MetacatContextManager.getContext().setRequestName(resourceRequestName);
            log.info("### Calling method: {}", resourceRequestName);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            percentileTags.put("request_error", "true");
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation. " + e.getMessage());
        } catch (IllegalArgumentException e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            percentileTags.put("request_error", "true");
            log.error(e.getMessage(), e);
            throw new MetacatBadRequestException(String.format("%s.%s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage()));
        } catch (Exception e) {
            collectRequestExceptionMetrics(tags, e.getClass().getSimpleName());
            percentileTags.put("request_error", "true");
            final String message = String
                .format("%s.%s -- %s failed.",
                    e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage(),
                    resourceRequestName);
            log.error(message, e);
            if (e instanceof MetacatException) {
                throw e;
            } else {
                throw new MetacatException(message, e);
            }
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("### Time taken to complete {} is {} ms", resourceRequestName,
                duration);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
            PercentileTimer.get(this.registry, requestPercentileTimerId.withTags(percentileTags))
                .record(duration, TimeUnit.MILLISECONDS);
        }
    }

    private void collectRequestExceptionMetrics(final Map<String, String> tags, final String exceptionName) {
        tags.put("exception", exceptionName);
        registry.counter(requestFailureCounterId.withTags(tags)).increment();
    }
}
