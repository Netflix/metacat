package com.netflix.metacat.main.services;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import lombok.NonNull;

import javax.annotation.Nullable;

/**
 * Interface for validating table owner attribute.
 */
public interface OwnerValidationService {
    /**
     * Checks whether the given owner is valid against a registry.
     *
     * @param user the user
     * @return true if the owner is valid, else false
     */
    boolean isUserValid(@Nullable String user);

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
