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
package com.netflix.metacat.canonical.common.exception;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

/**
 * Error code class.
 * @author zhenl
 */
@Getter
@ToString
@EqualsAndHashCode
public final class ErrorCode {
    private final int code;
    private final String name;

    /**
     * Constructor.
     * @param code code
     * @param name name
     */
    @JsonCreator
    public ErrorCode(@JsonProperty("code") final int code, @JsonProperty("name") final String name) {
        if (code < 0) {
            throw new IllegalArgumentException("code is negative");
        }
        this.code = code;
        this.name = Objects.requireNonNull(name, "name is null");
    }

}
