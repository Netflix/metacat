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
package com.netflix.metacat.common.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Abstract type class.
 *
 * @author zhenl
 */
@Getter
@EqualsAndHashCode
public abstract class AbstractType implements Type {
    private final TypeSignature typeSignature;

    AbstractType(final TypeSignature typeSignature) {
        this.typeSignature = typeSignature;
    }

    /**
     * get display name.
     *
     * @return name
     */
    public String getDisplayName() {
        return typeSignature.toString();
    }
}
