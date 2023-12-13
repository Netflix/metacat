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

package com.netflix.metacat.common.dto

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.reflect.ClassPath
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.SerializationUtils
import spock.lang.Specification
import spock.lang.Unroll
import java.beans.Introspector
import java.beans.PropertyDescriptor
import java.lang.reflect.*
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import java.util.stream.Collectors

class DtoVerificationSpec extends Specification {
    private static final Random rand = new Random()
    private static final Reflections reflections = new Reflections(BaseDto.class.package.name, new SubTypesScanner(false))
    private static final MetacatJson metacatJson = new MetacatJsonLocator()

    public static Set<Class<?>> getDtoClasses() {
        def classes = reflections.getSubTypesOf(Object.class).stream().filter {theClass ->
            int modifiers = theClass.modifiers
            return theClass.name.endsWith('Dto') &&
                Modifier.isPublic(modifiers) &&
                !Modifier.isAbstract(modifiers) &&
                !Modifier.isInterface(modifiers)
        }.collect(Collectors.toSet())

        return classes
    }

    public static Set<Class<?>> getHasDataMetadataClasses() {
        return getDtoClasses().findAll { Class<?> theClass ->
            return HasDataMetadata.isAssignableFrom(theClass)
        }
    }

    public static Set<Class<?>> getHasQualifiedNameClasses() {
        return getDtoClasses().findAll { Class<?> theClass ->
            return theClass.properties.methods.any { Method method ->
                method.returnType == QualifiedName
            }
        }
    }

    public static <T> T getRandomDtoInstance(Class<T> clazz) {
        T dto = clazz.newInstance()

        def propertyDescriptors = Introspector.getBeanInfo(clazz).propertyDescriptors
        for (PropertyDescriptor descriptor : propertyDescriptors) {
            Method writeMethod = descriptor.writeMethod
            if (writeMethod) {
                Class<?> type = descriptor.propertyType
                Field field = clazz.declaredFields.find { it.name == descriptor.name }
                if (field) {
                    def randomValue = getRandomValue(type, field)
                    writeMethod.invoke(dto, randomValue)
                } else if (!['dataExternal', 'partition_keys'].contains(descriptor.name)) {
                    System.out.println("Unable to locate field for descriptor ${descriptor}")
                }
            }
        }

        return dto
    }

    public static <T> T getRandomValue(Class<T> clazz) {
        return getRandomValue(clazz, null)
    }

    @SuppressWarnings("GroovyAssignabilityCheck")
    public static <T> T getRandomValue(Class<T> clazz, Field field) {
        switch (clazz) {
            case String:
                return RandomStringUtils.randomAlphabetic(4)
            case Boolean:
            case Boolean.TYPE:
                return rand.nextBoolean()
            case Integer:
            case Integer.TYPE:
                return rand.nextInt()
            case Long:
            case Long.TYPE:
                return rand.nextLong()
            case Float:
            case Float.TYPE:
                return rand.nextFloat()
            case Double:
            case Double.TYPE:
                return rand.nextDouble()
            case List:
                assert field.genericType instanceof ParameterizedType, "Field ${field} is not generic"
                ParameterizedType type = field.genericType as ParameterizedType
                return (0..3).collect {
                    return getRandomValue(Class.forName(type.actualTypeArguments[0].typeName))
                }
            case Map:
                assert field.genericType instanceof ParameterizedType, "Field ${field} is not generic"
                ParameterizedType type = field.genericType as ParameterizedType
                def map = [:]
                (0..3).each {
                    Type keyType = type.actualTypeArguments[0]
                    Type valueType = type.actualTypeArguments[1]
                    assert keyType.typeName == 'java.lang.String', "Currently only handle string keys"
                    map.put(RandomStringUtils.randomAlphabetic(4), getRandomValue(Class.forName(valueType.typeName)))
                }
                return map
            case ObjectNode:
                ObjectNode node = metacatJson.emptyObjectNode()
                node.put(RandomStringUtils.randomAlphabetic(4), RandomStringUtils.randomAlphabetic(4))
                return node
            case JsonNode:
                ObjectNode node = metacatJson.emptyObjectNode()
                node.put(RandomStringUtils.randomAlphabetic(4), RandomStringUtils.randomAlphabetic(4))
                return node
            case Date:
                return new Date(rand.nextLong())
            case QualifiedName:
                return QualifiedName.ofCatalog(RandomStringUtils.randomAlphabetic(4))
            case { clazz.package.name.startsWith(BaseDto.class.package.name) }:
                return getRandomDtoInstance(clazz)
            default:
                throw new IllegalStateException("Unsure how to get a random instance of class: ${clazz}")
        }
    }

    def 'can get dto classes'() {
        expect:
        dtoClasses.size() >= 1
    }

    @Unroll
    def 'test serialization of #clazz'() {
        expect:
        assert clazz instanceof Serializable, "Unable to serialize ${clazz}"
        Serializable randomInstance1 = getRandomDtoInstance(clazz) as Serializable

        SerializationUtils.roundtrip(randomInstance1).toString() == randomInstance1.toString()

        where:
        clazz << dtoClasses
    }
}
