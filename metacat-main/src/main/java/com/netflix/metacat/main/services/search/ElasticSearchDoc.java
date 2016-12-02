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

package com.netflix.metacat.main.services.search;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import lombok.Getter;
import org.elasticsearch.action.get.GetResponse;

import java.util.Map;

/**
 * Document that gets stored in elastic search.
 * @author amajumdar
 */
@Getter
public class ElasticSearchDoc {
    private String id;
    private Object dto;
    private String user;
    private boolean deleted;
    private String refreshMarker;

    /**
     * Constructor.
     * @param id doc id
     * @param dto dto
     * @param user user name
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
     * @param id doc id
     * @param dto dto
     * @param user user name
     * @param deleted is it marked deleted
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

    private static Class getClass(final String type) {
        return Type.valueOf(type).getClazz();
    }

    /**
     * Parse the elastic search response.
     * @param response response
     * @return document
     */
    public static ElasticSearchDoc parse(final GetResponse response) {
        ElasticSearchDoc result = null;
        if (response.isExists()) {
            final Map<String, Object> responseMap = response.getSourceAsMap();
            final String user = (String) responseMap.get(Field.USER);
            final boolean deleted = (boolean) responseMap.get(Field.DELETED);
            @SuppressWarnings("unchecked")
            final Object dto = MetacatJsonLocator.INSTANCE.parseJsonValue(
                response.getSourceAsBytes(),
                getClass(response.getType())
            );
            result = new ElasticSearchDoc(response.getId(), dto, user, deleted);
        }
        return result;
    }

    private ObjectNode toJsonObject() {
        final ObjectNode oMetadata = MetacatJsonLocator.INSTANCE.toJsonObject(dto);
        //True if this entity has been deleted
        oMetadata.put(Field.DELETED, deleted);
        //True if this entity has been deleted
        oMetadata.put(Field.USER, user);
        if (refreshMarker != null) {
            oMetadata.put(Field.REFRESH_MARKER, refreshMarker);
        }
        return oMetadata;
    }

    String toJsonString() {
        final String result = MetacatJsonLocator.INSTANCE.toJsonString(toJsonObject());
        return result.replace("{}", "null");
    }

    /** Document types. */
    public enum Type {
        /** Document types. */
        catalog(CatalogDto.class), database(DatabaseDto.class), table(TableDto.class),
        /** Document types. */
        mview(TableDto.class), partition(PartitionDto.class);

        private Class clazz;

        Type(final Class clazz) {
            this.clazz = clazz;
        }

        public Class getClazz() {
            return clazz;
        }
    }

    /** Document context attributes. */
    protected static class Field {
        public static final String USER = "user_";
        public static final String DELETED = "deleted_";
        public static final String REFRESH_MARKER = "refreshMarker_";
    }
}
