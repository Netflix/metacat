/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.type.TypeSignature;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Field DTO.
 *
 * @author amajumdar
 * @since 1.0.0
 */
@Schema(description = "Table field/column metadata")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@Builder
@AllArgsConstructor
public final class FieldInfo implements Serializable {
    private static final long serialVersionUID = -762764635047711004L;
    private String comment;
    private String name;
    private boolean partitionKey;
    private String sourceType;
    private transient Type type;
    private Boolean isNullable;
    private Integer size;
    private String defaultValue;
    private Boolean isSortKey;
    private Boolean isIndexKey;

    // setters and getters
    private void writeObject(final ObjectOutputStream oos)
        throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(type == null ? null : type.getDisplayName());
    }

    private void readObject(final ObjectInputStream ois)
        throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        final Object oSignature = ois.readObject();
        if (oSignature != null) {
            final String signatureString = (String) oSignature;
            if (StringUtils.isNotBlank(signatureString)) {
                final TypeSignature signature = TypeSignature.parseTypeSignature(signatureString);
                this.setType((TypeRegistry.getTypeRegistry().getType(signature)));
            }
        }
    }
}
