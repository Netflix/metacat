package com.netflix.metacat.common.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.HasMetadata;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface UserMetadataService {
    String METACAT_USERMETADATA_CONFIG_LOCATION = "metacat.usermetadata.config.location";

    void deleteDataMetadatas(@Nonnull List<String> uris);

    void deleteDefinitionMetadatas(@Nonnull List<QualifiedName> names);

    void deleteMetadatas(List<HasMetadata> holders, boolean force);

    @Nonnull
    Optional<ObjectNode> getDataMetadata(@Nonnull String uri);

    @Nonnull
    Map<String, ObjectNode> getDataMetadataMap(@Nonnull List<String> uris);

    @Nonnull
    Optional<ObjectNode> getDefinitionMetadata(@Nonnull QualifiedName name);

    @Nonnull
    Map<String, ObjectNode> getDefinitionMetadataMap(@Nonnull List<QualifiedName> names);

    void saveDataMetadata(@Nonnull String uri, @Nonnull String userId, @Nonnull Optional<ObjectNode> metadata, boolean merge);

    void saveDefinitionMetadata(@Nonnull QualifiedName name, @Nonnull String userId,
            @Nonnull Optional<ObjectNode> metadata, boolean merge);

    void saveMetadata(String userId, HasMetadata holder, boolean merge);

    void populateMetadata(HasMetadata holder);

    void populateMetadata(HasMetadata holder, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    int renameDataMetadataKey(@Nonnull String oldUri, @Nonnull String newUri);

    int renameDefinitionMetadataKey(@Nonnull QualifiedName oldName, @Nonnull QualifiedName newName);

    void start() throws Exception;

    void stop() throws Exception;

    void saveMetadatas(String user, List<? extends HasMetadata> holders, boolean merge);

    List<DefinitionMetadataDto> searchDefinitionMetadatas(Set<String> propertyNames, String type, String name
                                , String sortBy, String sortOrder, Integer offset, Integer limit);

    List<QualifiedName> searchByOwners(Set<String> owners);
}
