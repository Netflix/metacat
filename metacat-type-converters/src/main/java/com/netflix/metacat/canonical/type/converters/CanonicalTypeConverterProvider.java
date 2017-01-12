///*
// * Copyright 2016 Netflix, Inc.
// *    Licensed under the Apache License, Version 2.0 (the "License");
// *    you may not use this file except in compliance with the License.
// *    You may obtain a copy of the License at
// *        http://www.apache.org/licenses/LICENSE-2.0
// *    Unless required by applicable law or agreed to in writing, software
// *    distributed under the License is distributed on an "AS IS" BASIS,
// *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *    See the License for the specific language governing permissions and
// *    limitations under the License.
// */
//
//package com.netflix.metacat.canonical.type.converters;
//
//import javax.inject.Inject;
//import javax.inject.Provider;
//
//import com.netflix.metacat.common.MetacatRequestContext;
//import com.netflix.metacat.common.util.MetacatContextManager;
//
///**
// * Canonical Type Converter Provider class.
// */
//public class CanonicalTypeConverterProvider implements Provider<CanonicalTypeConverter> {
//    private final CanonicalHiveTypeConverter hiveTypeConverter;
//    private final CanonicalPigTypeConverter pigTypeConverter;
//
//    /**
//     * Constructor.
//     *
//     * @param hiveTypeConverter hive type converter
//     * @param pigTypeConverter  pig type converter
//     */
//    @Inject
//    public CanonicalTypeConverterProvider(
//        final CanonicalHiveTypeConverter hiveTypeConverter,
//        final CanonicalPigTypeConverter pigTypeConverter
//    ) {
//        this.hiveTypeConverter = hiveTypeConverter;
//        this.pigTypeConverter = pigTypeConverter;
//    }
//
//    @Override
//    public CanonicalTypeConverter get() {
//        final MetacatRequestContext requestContext = MetacatContextManager.getContext();
//        final MetacatRequestContext.DataTypeContext dataTypeContext = requestContext.getDataTypeContext();
//
//        if (dataTypeContext == null) {
//            return this.getDefaultConverter();
//        } else {
//            return this.get(dataTypeContext);
//        }
//    }
//
//    /**
//     * Returns the right type converter based on the context.
//     *
//     * @param context context
//     * @return type converter
//     */
//    public CanonicalTypeConverter get(final MetacatRequestContext.DataTypeContext context) {
//        switch (context) {
//            case hive:
//                return this.hiveTypeConverter;
//            case pig:
//                return this.pigTypeConverter;
//            default:
//                throw new IllegalArgumentException("No handler for " + context);
//        }
//    }
//
//    private CanonicalTypeConverter getDefaultConverter() {
//        return this.pigTypeConverter;
//    }
//
//    /**
//     * Returns the default converter.
//     *
//     * @return converter
//     */
//    public MetacatRequestContext.DataTypeContext getDefaultConverterType() {
//        final CanonicalTypeConverter converter = getDefaultConverter();
//        if (converter instanceof CanonicalHiveTypeConverter) {
//            return MetacatRequestContext.DataTypeContext.hive;
//        } else if (converter instanceof CanonicalPigTypeConverter) {
//            return MetacatRequestContext.DataTypeContext.pig;
//        } else {
//            throw new IllegalStateException("Unknown handler: " + converter);
//        }
//    }
//}
