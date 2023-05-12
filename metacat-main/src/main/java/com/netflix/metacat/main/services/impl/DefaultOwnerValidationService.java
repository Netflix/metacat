package com.netflix.metacat.main.services.impl;

import com.google.common.collect.ImmutableSet;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.services.OwnerValidationService;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A default implementation of Ownership validation service that check for users against
 * known invalid userIds.
 */
@Slf4j
@RequiredArgsConstructor
public class DefaultOwnerValidationService implements OwnerValidationService {
    private static final Set<String> KNOWN_INVALID_OWNERS = ImmutableSet.of(
        "root", "metacat", "metacat-thrift-interface");
    private static final Set<String> KNOWN_INVALID_OWNER_GROUPS = ImmutableSet.of(
        "root", "metacat", "metacat-thrift-interface");

    private final Registry registry;

    @Override
    public List<String> extractPotentialOwners(@NonNull final TableDto dto) {
        return Stream.of(
            dto.getTableOwner().orElse(null),
            MetacatContextManager.getContext().getUserName(),
            dto.getSerde().getOwner()
        ).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public List<String> extractPotentialOwnerGroups(@NonNull final TableDto dto) {
        return Collections.singletonList(dto.getTableOwnerGroup().orElse(null));
    }

    @Override
    public boolean isUserValid(@Nullable final String user) {
        return !isKnownInvalidUser(user);
    }

    @Override
    public boolean isGroupValid(@Nullable final String groupName) {
        return !isKnownInvalidGroup(groupName);
    }

    @Override
    public void enforceOwnerValidation(@NonNull final String operationName,
                                       @NonNull final QualifiedName tableName,
                                       @NonNull final TableDto tableDto) {
        final String tableOwner = tableDto.getTableOwner().orElse(null);
        final String tableOwnerGroup = tableDto.getTableOwnerGroup().orElse(null);

        final MetacatRequestContext context = MetacatContextManager.getContext();
        final Map<String, String> requestHeaders = getHttpHeaders();

        final boolean tableOwnerValid = isUserValid(tableOwner) || isGroupValid(tableOwnerGroup);

        logOwnershipDiagnosticDetails(
            operationName, tableName, tableDto,
            context, tableOwnerValid, requestHeaders);
    }

    /**
     * Checks if the user is from a known list of invalid users. Subclasses can use
     * this method before attempting to check against remote services to save on latency.
     *
     * @param userId the user id
     * @return true if the user id is a known invalid user, else false
     */
    protected boolean isKnownInvalidUser(@Nullable final String userId) {
        return StringUtils.isBlank(userId) || knownInvalidOwners().contains(userId);
    }

    /**
     * Checks if the group is from a known list of invalid groups. Subclasses can use
     * this method before attempting to check against remote services to save on latency.
     *
     * @param groupName the group name
     * @return true if the group is a known invalid group, else false
     */
    protected boolean isKnownInvalidGroup(@Nullable final String groupName) {
        return StringUtils.isBlank(groupName) || knownInvalidOwnerGroups().contains(groupName);
    }

    /**
     * Returns set of known invalid users. Subclasses can override to provide
     * a list fetched from a dynamic source.
     *
     * @return set of known invalid users
     */
    protected Set<String> knownInvalidOwners() {
        return KNOWN_INVALID_OWNERS;
    }

    /**
     * Returns set of known invalid owner groups. Subclasses can override to provide
     * a list fetched from a dynamic source.
     *
     * @return set of known invalid groups
     */
    protected Set<String> knownInvalidOwnerGroups() {
        return KNOWN_INVALID_OWNER_GROUPS;
    }

    /**
     * Logs diagnostic data for debugging invalid owners. Subclasses can use this to log
     * diagnostic data when owners are found to be invalid.
     */
    protected void logOwnershipDiagnosticDetails(final String operationName,
                                                 final QualifiedName name,
                                                 final TableDto tableDto,
                                                 final MetacatRequestContext context,
                                                 final boolean tableOwnerValid,
                                                 final Map<String, String> requestHeaders) {
        try {
            if (!tableOwnerValid) {
                registry.counter(
                    "metacat.table.owner.invalid",
                    "operation", operationName,
                    "scheme", String.valueOf(context.getScheme()),
                    "catalogAndDb", name.getCatalogName() + "_" + name.getDatabaseName()
                ).increment();

                log.info("Operation: {}, invalid owner: {}, group: {}. name: {}, dto: {}, context: {}, headers: {}",
                    operationName,
                    tableDto.getTableOwner().orElse("<null>"),
                    tableDto.getTableOwnerGroup().orElse("<null>"),
                    name, tableDto, context, requestHeaders);
            }
        } catch (final Exception ex) {
            log.warn("Error when logging diagnostic data for invalid owner for operation: {}, name: {}, table: {}",
                operationName, name, tableDto, ex);
        }
    }

    /**
     * Returns all the Http headers for the current request. Subclasses can use it to
     * log diagnostic data.
     *
     * @return the Http headers
     */
    protected Map<String, String> getHttpHeaders() {
        final Map<String, String> requestHeaders = new HashMap<>();

        final RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();

        if (requestAttributes instanceof ServletRequestAttributes) {
            final ServletRequestAttributes servletRequestAttributes = (ServletRequestAttributes) requestAttributes;

            final HttpServletRequest servletRequest = servletRequestAttributes.getRequest();

            if (servletRequest != null) {
                final Enumeration<String> headerNames = servletRequest.getHeaderNames();

                if (headerNames != null) {
                    while (headerNames.hasMoreElements()) {
                        final String header = headerNames.nextElement();
                        requestHeaders.put(header, servletRequest.getHeader(header));
                    }
                }
            }
        }

        return requestHeaders;
    }
}
