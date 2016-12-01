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

package com.netflix.metacat.common.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * JSON utility.
 */
public interface MetacatJson {
    /**
     * Convenience method for doing two-step conversion from given value, into
     * instance of given value type. This is functionality equivalent to first
     * serializing given value into JSON, then binding JSON data into value
     * of given type, but may be executed without fully serializing into
     * JSON. Same converters (serializers, deserializers) will be used as for
     * data binding, meaning same object mapper configuration works.
     *
     * @param fromValue object to be converted
     * @param toValueType POJO class to be converted to
     * @param <T> POJO class
     * @return Returns the converted POJO
     * @throws MetacatJsonException If conversion fails due to incompatible type;
     *    if so, root cause will contain underlying checked exception data binding
     *    functionality threw
     */
    <T> T convertValue(Object fromValue, Class<T> toValueType);

    /**
     * A helper for implementing Serializable.  Reads a boolean to from the inputStream to determine of the next
     * object is a json object and if it is it reads it and returns an object node.
     *
     * @param inputStream the serilization input stream
     * @return a json object if one is the next object otherwise null
     * @throws IOException on an error reading from the stream or a json serilization error.
     */
    @Nullable
    ObjectNode deserializeObjectNode(
        @Nonnull
            ObjectInputStream inputStream) throws IOException;

    /**
     * Returns an empty object node.
     * @return an empty object node
     */
    ObjectNode emptyObjectNode();

    /**
     * Returns default ObjectMapper used by this instance.
     * @return The default ObjectMapper used by this instance.
     */
    ObjectMapper getObjectMapper();

    /**
     * Returns default ObjectMapper used by this instance configured to pretty print.
     * @return The default ObjectMapper used by this instance configured to pretty print.
     */
    ObjectMapper getPrettyObjectMapper();

    /**
     * Merge primary and additional json nodes.
     * @param primary first json node
     * @param additional second json node
     */
    void mergeIntoPrimary(
        @Nonnull
            ObjectNode primary,
        @Nonnull
            ObjectNode additional);

    /**
     * Parses the given string as json and returns an ObjectNode representing the json.  Assumes the json is of a
     * json object
     *
     * @param s a string representing a json object
     * @return an object node representation of the string
     * @throws MetacatJsonException if unable to convert the string to json or the json isn't a json object.
     */
    ObjectNode parseJsonObject(String s);

    /**
     * Parses the given JSON value.
     * @param s json string
     * @param clazz class
     * @param <T> type of the class
     * @return object
     */
    <T> T parseJsonValue(String s, Class<T> clazz);

    /**
     * Parses the given JSON value.
     * @param s json byte array
     * @param clazz class
     * @param <T> type of the class
     * @return object
     */
    <T> T parseJsonValue(byte[] s, Class<T> clazz);

    /**
     * Serializes the JSON.
     * @param outputStream output stream
     * @param json json node
     * @throws IOException exception
     */
    void serializeObjectNode(
        @Nonnull
            ObjectOutputStream outputStream,
        @Nullable
            ObjectNode json) throws IOException;

    /**
     * Converts JSON as bytes.
     * @param o object
     * @return byte array
     */
    byte[] toJsonAsBytes(Object o);

    /**
     * Converts an object to JSON.
     * @param o object
     * @return JSON node
     */
    ObjectNode toJsonObject(Object o);

    /**
     * Converts an object to JSON string.
     * @param o object
     * @return JSON string
     */
    String toJsonString(Object o);
}
