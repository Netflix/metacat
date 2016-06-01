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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonException;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import feign.Response;
import feign.RetryableException;
import feign.Util;

import java.io.IOException;

import static javax.ws.rs.core.Response.Status;

/**
 * Module that provides a error decoder, used to parse errors
 */
public class MetacatErrorDecoder extends feign.codec.ErrorDecoder.Default {
    private static final MetacatJson metacatJson = MetacatJsonLocator.INSTANCE;
        @Override
        public Exception decode(String methodKey, Response response){
            try {
                String message = "";
                if (response.body() != null){
                    message = Util.toString(response.body().asReader());
                    try {
                        ObjectNode body = metacatJson.parseJsonObject(message);
                        message = body.path("error").asText();
                    } catch (MetacatJsonException ignored) {}
                }
                Status status = Status.fromStatusCode(response.status());
                //Status codes(ex:502) that cannot be resolved to a JAX-RS Response Status will return a MetacatException
                if( status == null){
                    return new MetacatException(message, response.status(), null);
                }
                switch (status) {
                    case UNSUPPORTED_MEDIA_TYPE:
                        return new MetacatNotSupportedException(message);
                    case BAD_REQUEST:
                        return new MetacatBadRequestException(message);
                    case NOT_FOUND:
                        return new MetacatNotFoundException(message);
                    case CONFLICT:
                        return new MetacatAlreadyExistsException(message);
                    case INTERNAL_SERVER_ERROR:
                    case SERVICE_UNAVAILABLE:
                        return new RetryableException(message, null);
                    default:
                        return new MetacatException(message, Status.INTERNAL_SERVER_ERROR, null);
                }
            } catch (IOException e) {
                return super.decode(methodKey, response);
            }
        }
}
