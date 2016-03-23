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

package com.netflix.metacat.client.module;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.RequestTemplate;
import feign.codec.EncodeException;
import feign.codec.Encoder;

import java.lang.reflect.Type;

public class JacksonEncoder implements Encoder {
    private final ObjectMapper mapper;

    public JacksonEncoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Converts objects to an appropriate representation in the template.
     *
     * @param object   what to encode as the request body.
     * @param bodyType the type the object should be encoded as. {@code Map<String, ?>}, if form
     *                 encoding.
     * @param template the request template to populate.
     * @throws feign.codec.EncodeException when encoding failed due to a checked exception.
     */
    @Override
    public void encode(Object object, Type bodyType, RequestTemplate template) throws EncodeException {
        try {
            template.body(mapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            throw new EncodeException(e.getMessage(), e);
        }
    }
}
