package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for validating table owner attribute.
 */
public interface OwnerValidationService {
    /**
     * Returns an ordered list of owners to use used for validation and owner assignment. Since metacat owners
     * in a request may come from a number of places (DTO, Request context) this method centralizes that order.
     *
     * @param dto the input Table Dto
     * @return an ordered list of owners to use used for validation and owner assignment
     */
    List<String> extractPotentialOwners(@NonNull TableDto dto);

    /**
     * Returns an ordered list of owner groups to use used for validation and owner assignment. Since metacat owners
     * in a request may come from a number of places (DTO, Request context) this method centralizes that order.
     *
     * @param dto the input Table Dto
     * @return an ordered list of owner groups to use used for validation and owner assignment
     */
    List<String> extractPotentialOwnerGroups(@NonNull TableDto dto);

    /**
     * Checks whether the given owner is valid against a registry.
     *
     * @param user the user
     * @return true if the owner is valid, else false
     */
    boolean isUserValid(@Nullable String user);

    /**
     * Checks whether the given owner group is valid against a registry.
     *
     * @param groupName the groupName
     * @return true if the owner group is valid, else false
     */
    boolean isGroupValid(@Nullable String groupName);

    /**
     * Enforces valid table owner attribute. Implementations are free to
     * handle it as needed - throw exceptions or ignore. The owner attribute
     * in the DTO may or may not be valid so implementations should check for validity
     * before enforcement.
     *
     * @param operationName the name of the metacat API, useful for logging
     * @param tableName the name of the table
     * @param tableDto the table dto containing the owner in the definition metadata field
     */
    void enforceOwnerValidation(@NonNull String operationName,
                                @NonNull QualifiedName tableName,
                                @NonNull TableDto tableDto);
}
