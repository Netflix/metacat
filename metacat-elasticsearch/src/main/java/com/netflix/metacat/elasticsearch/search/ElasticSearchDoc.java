/*
 *       Copyright 2017 Netflix, Inc.
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

package com.netflix.metacat.elasticsearch.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import lombok.Getter;

/**
 * Document that gets stored in elastic search.
 *
 * @author amajumdar
 */
@Getter
public class ElasticSearchDoc {
    /**
     * Definition Metadata pull out fields.
     */
    private static final String[] DEFINITION_METADATA_FIELDS = {
            ElasticSearchDocConstants.DEFINITION_METADATA_OWNER,
            ElasticSearchDocConstants.DEFINITION_METADATA_TAGS,
            ElasticSearchDocConstants.DEFINITION_METADATA_DATA_HYGIENE,
            ElasticSearchDocConstants.DEFINITION_METADATA_LIFETIME,
            ElasticSearchDocConstants.DEFINITION_METADATA_EXTENDED_SCHEMA,
            ElasticSearchDocConstants.DEFINITION_METADATA_DATA_DEPENDENCY,
            ElasticSearchDocConstants.DEFINITION_METADATA_TABLE_COST,
            ElasticSearchDocConstants.DEFINITION_METADATA_LIFECYCLE,
            ElasticSearchDocConstants.DEFINITION_METADATA_AUDIENCE,
            ElasticSearchDocConstants.DEFINITION_METADATA_MODEL,
            ElasticSearchDocConstants.DEFINITION_METADATA_SUBJECT_AREA,
            ElasticSearchDocConstants.DEFINITION_METADATA_DATA_CATEGORY,
            ElasticSearchDocConstants.DEFINITION_METADATA_JOB,
            ElasticSearchDocConstants.DEFINITION_METADATA_TABLE_DESCRIPTION,
    };

    private String id;
    private Object dto;
    private String user;
    private boolean deleted;
    private String refreshMarker;
    private boolean addSearchableDefinitionMetadataEnabled = true;
    private MetacatJson metacatJson = new MetacatJsonLocator();

    /**
     * Constructor.
     *
     * @param id      doc id
     * @param dto     dto
     * @param user    user name
     * @param deleted is it marked deleted
     */
    public ElasticSearchDoc(final String id, final Object dto, final String user, final boolean deleted) {
        this.id = id;
        this.dto = dto;
        this.user = user;
        this.deleted = deleted;
    }

    /**
     * Constructor.
     *
     * @param id            doc id
     * @param dto           dto
     * @param user          user name
     * @param deleted       is it marked deleted
     * @param refreshMarker marker
     */
    public ElasticSearchDoc(final String id, final Object dto, final String user, final boolean deleted,
                            final String refreshMarker) {
        this.id = id;
        this.dto = dto;
        this.user = user;
        this.deleted = deleted;
        this.refreshMarker = refreshMarker;
    }

    private ObjectNode toJsonObject() {
        final ObjectNode oMetadata = metacatJson.toJsonObject(dto);
        if (addSearchableDefinitionMetadataEnabled) {
            //add the searchable definition metadata
            addSearchableDefinitionMetadata(oMetadata);
        }
        //True if this entity has been deleted
        oMetadata.put(Field.DELETED, deleted);
        //True if this entity has been deleted
        oMetadata.put(Field.USER, user);
        if (refreshMarker != null) {
            oMetadata.put(Field.REFRESH_MARKER, refreshMarker);
        }
        return oMetadata;
    }

    /**
     * addSearchableDefinitionMetadataEnabled.
     *
     * @param objectNode object node
     */
    public void addSearchableDefinitionMetadata(final ObjectNode objectNode) {
        final JsonNode jsonNode = objectNode.get(ElasticSearchDocConstants.DEFINITION_METADATA);
        final ObjectNode node = JsonNodeFactory.instance.objectNode();
        for (final String tag : DEFINITION_METADATA_FIELDS) {
            node.set(tag, jsonNode.get(tag));
        }
        objectNode.set(Field.SEARCHABLE_DEFINITION_METADATA, node);
    }

    String toJsonString() {
        final String result = metacatJson.toJsonString(toJsonObject());
        return result.replace("{}", "null");
    }

    /**
     * Document types.
     */
    public enum Type {
        /**
         * Document types.
         */
        catalog(CatalogDto.class), database(DatabaseDto.class), table(TableDto.class),
        /**
         * Document types.
         */
        mview(TableDto.class), partition(PartitionDto.class);

        private Class clazz;

        Type(final Class clazz) {
            this.clazz = clazz;
        }

        public Class getClazz() {
            return clazz;
        }
    }

    /**
     * Document context attributes.
     */
    protected static class Field {
        public static final String USER = "user_";
        public static final String DELETED = "deleted_";
        public static final String REFRESH_MARKER = "refreshMarker_";
        public static final String SEARCHABLE_DEFINITION_METADATA = "searchableDefinitionMetadata";
    }


}
