/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.main.api;

import com.facebook.presto.exception.InvalidMetaException;
import com.facebook.presto.exception.PartitionAlreadyExistsException;
import com.facebook.presto.exception.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.common.usermetadata.UserMetadataServiceException;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.DynamicTimer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RequestWrapper {
    private static final Logger log = LoggerFactory.getLogger(RequestWrapper.class);

    public static QualifiedName qualifyName(Supplier<QualifiedName> nameSupplier) {
        try {
            return nameSupplier.get();
        } catch (Exception e) {
            log.error("Invalid qualified name", e);
            throw new MetacatBadRequestException(e.getMessage());
        }
    }

    public static <R> R requestWrapper(
        QualifiedName name,
        String resourceRequestName,
        Supplier<R> supplier) {
        BasicTagList tags = getQualifiedNameTagList(name).copy("request", resourceRequestName);
        DynamicCounter.increment("dse.metacat.counter.requests", tags);
        Stopwatch timer = DynamicTimer.start("dse.metacat.timer.requests", tags);
        try {
            log.info("### Calling method: {} for {}", resourceRequestName, name);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation");
        } catch (SchemaAlreadyExistsException | TableAlreadyExistsException | PartitionAlreadyExistsException e) {
            log.error(e.getMessage(), e);
            throw new MetacatAlreadyExistsException(e.getMessage());
        } catch (NotFoundException | MetacatNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new MetacatNotFoundException(
                String.format("Unable to locate for %s. Details: %s", name, e.getMessage()));
        } catch (InvalidMetaException | IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            throw new MetacatBadRequestException(
                String.format("%s.%s", e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage()));
        } catch (PrestoException e) {
            String message = String.format("%s.%s -- %s failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            if (e.getErrorCode() == StandardErrorCode.NOT_SUPPORTED.toErrorCode()) {
                throw new MetacatNotSupportedException("Catalog does not support the operation");
            } else {
                DynamicCounter.increment("dse.metacat.counter.failure.requests", tags);
                throw new MetacatException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
            }
        } catch (UserMetadataServiceException e) {
            String message = String.format("%s.%s -- %s usermetadata operation failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            throw new MetacatUserMetadataException(message);
        } catch (Exception e) {
            DynamicCounter.increment("dse.metacat.counter.failure.requests", tags);
            String message = String.format("%s.%s -- %s failed for %s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            throw new MetacatException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
        } finally {
            timer.stop();
            log.info("### Time taken to complete {} is {} ms", resourceRequestName,
                timer.getDuration(TimeUnit.MILLISECONDS));
        }
    }

    private static BasicTagList getQualifiedNameTagList(QualifiedName name) {
        Map<String, String> tags = Maps.newHashMap(name.toJson());
        tags.remove("qualifiedName");
        return BasicTagList.copyOf(tags);
    }

    public static <R> R requestWrapper(
        String resourceRequestName,
        Supplier<R> supplier) {
        TagList tags = BasicTagList.of("request", resourceRequestName);
        DynamicCounter.increment("dse.metacat.counter.requests", tags);
        Stopwatch timer = DynamicTimer.start("dse.metacat.timer.requests", tags);
        try {
            log.info("### Calling method: {}", resourceRequestName);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation");
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            throw new MetacatBadRequestException(String.format("%s.%s", e.getMessage(),
                e.getCause() == null ? "" : e.getCause().getMessage()));
        } catch (Exception e) {
            DynamicCounter.increment("dse.metacat.counter.failure.requests", tags);
            String message = String
                .format("%s.%s -- %s failed.", e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage(),
                    resourceRequestName);
            log.error(message, e);
            throw new MetacatException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
        } finally {
            timer.stop();
            log.info("### Time taken to complete {} is {} ms", resourceRequestName,
                timer.getDuration(TimeUnit.MILLISECONDS));
        }
    }
}
