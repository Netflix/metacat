package com.netflix.metacat.main.api;

import com.facebook.presto.exception.InvalidMetaException;
import com.facebook.presto.exception.PartitionAlreadyExistsException;
import com.facebook.presto.exception.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.exception.MetacatUserMetadataException;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.monitoring.TimerWrapper;
import com.netflix.metacat.common.usermetadata.UserMetadataServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
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
        TimerWrapper timer = TimerWrapper.createStarted("dse.metacat.timer." + resourceRequestName);
        CounterWrapper.incrementCounter("dse.metacat.counter." + resourceRequestName);
        try {
            log.info("### Calling method: {} for {}", resourceRequestName, name);
            return supplier.get();
        } catch (UnsupportedOperationException e) {
            log.error(e.getMessage(), e);
            throw new MetacatNotSupportedException("Catalog does not support the operation");
        } catch (SchemaAlreadyExistsException | TableAlreadyExistsException |PartitionAlreadyExistsException e) {
            log.error(e.getMessage(), e);
            throw new MetacatAlreadyExistsException(e.getMessage());
        } catch (NotFoundException|MetacatNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new MetacatNotFoundException("Unable to locate: " + name);
        } catch (InvalidMetaException | IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            throw new MetacatBadRequestException(String.format("%s.%s",e.getMessage(), e.getCause()==null?"":e.getCause().getMessage()));
        } catch (PrestoException e) {
            String message = String.format("%s.%s -- %s failed for %s", e.getMessage(), e.getCause()==null?"":e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            if (e.getErrorCode() == StandardErrorCode.NOT_SUPPORTED.toErrorCode()) {
                throw new MetacatNotSupportedException("Catalog does not support the operation");
            } else {
                CounterWrapper.incrementCounter("dse.metacat.counter.failure." + resourceRequestName);
                throw new MetacatException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
            }
        } catch(UserMetadataServiceException e){
            String message = String.format("%s.%s -- %s usermetadata operation failed for %s", e.getMessage(), e.getCause()==null?"":e.getCause().getMessage(), resourceRequestName, name);
            throw new MetacatUserMetadataException(message);
        } catch (Exception e) {
            CounterWrapper.incrementCounter("dse.metacat.counter.failure." + resourceRequestName);
            String message = String.format("%s.%s -- %s failed for %s", e.getMessage(), e.getCause()==null?"":e.getCause().getMessage(), resourceRequestName, name);
            log.error(message, e);
            throw new MetacatException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
        } finally {
            log.info("### Time taken to complete {} is {} ms", resourceRequestName, timer.stop());
        }
    }

    public static <R> R requestWrapper(
            String resourceRequestName,
            Supplier<R> supplier) {
        TimerWrapper timer = TimerWrapper.createStarted("dse.metacat.timer." + resourceRequestName);
        CounterWrapper.incrementCounter("dse.metacat.counter." + resourceRequestName);
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
            CounterWrapper.incrementCounter("dse.metacat.counter.failure." + resourceRequestName);
            String message = String.format("%s.%s -- %s failed.", e.getMessage(), e.getCause()==null?"":e.getCause().getMessage(), resourceRequestName);
            log.error(message, e);
            throw new MetacatException(message, Response.Status.INTERNAL_SERVER_ERROR, e);
        } finally {
            log.info("### Time taken to complete {} is {} ms", resourceRequestName, timer.stop());
        }
    }
}
