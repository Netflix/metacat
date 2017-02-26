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

package com.netflix.metacat.common.type;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Abstract type class.
 * @author zhenl
 */
@EqualsAndHashCode
public abstract class AbstractType implements Type {
    @Getter protected final TypeSignature signature;

    protected AbstractType(final TypeSignature signature) {
        this.signature = signature;
    }

    /**
     * getTypeSignature.
     * @return TypeSignature
     */
    public final TypeSignature getTypeSignature() {
        return signature;
    }

    /**
     * get display name.
     * @return name
     */
    public String getDisplayName() {
        return signature.toString();
    }
}
