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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
//import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Type signature class.
 *
 * @author zhenl
 */
@Getter
@EqualsAndHashCode
public class TypeSignature {
    protected final TypeEnum base;
    protected final List<TypeSignature> parameters;
    protected final List<Object> literalParameters;

    /**
     * Type signature constructor.
     *
     * @param base base type
     */
    public TypeSignature(@Nonnull @NonNull final TypeEnum base) {
        this.base = base;
        this.parameters = Lists.newArrayList();
        this.literalParameters = Lists.newArrayList();
    }

    /**
     * Type signature constructor.
     *
     * @param base              base type
     * @param parameters        type parameter
     * @param literalParameters literal parameter
     */
    public TypeSignature(
        @Nonnull @NonNull final TypeEnum base,
        @Nonnull @NonNull final List<TypeSignature> parameters,
        @Nullable final List<Object> literalParameters
    ) {
        if (literalParameters != null) {
            for (final Object literal : literalParameters) {
                if (!(literal instanceof String || literal instanceof Long)) {
                    throw new IllegalArgumentException(
                        String.format("Unsupported literal type: %s", literal.getClass()));
                }
            }
            this.literalParameters = ImmutableList.copyOf(literalParameters);
        } else {
            this.literalParameters = ImmutableList.copyOf(Lists.newArrayList());
        }
        this.base = base;
        this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
    }

    /**
     * Type signature constructor.
     *
     * @param base              base type
     * @param parameters        type parameter
     * @param literalParameters literal parameter
     */
    private TypeSignature(
        @Nonnull @NonNull final String base,
        @Nonnull @NonNull final List<TypeSignature> parameters,
        @Nullable final List<Object> literalParameters
    ) {
        this(TypeEnum.fromName(base), parameters, literalParameters);
    }

    /**
     * Parse Type Signature.
     *
     * @param signature signature string
     * @return TypeSignature
     */
    @JsonCreator
    public static TypeSignature parseTypeSignature(final String signature) {
        if (!signature.contains("<") && !signature.contains("(")) {
            return new TypeSignature(signature, new ArrayList<TypeSignature>(), new ArrayList<>());
        }

        String baseName = null;
        final List<TypeSignature> parameters = new ArrayList<>();
        final List<Object> literalParameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;
        boolean inLiteralParameters = false;

        for (int i = 0; i < signature.length(); i++) {
            final char c = signature.charAt(i);
            if (c == '<') {
                if (bracketCount == 0) {
                    if (baseName != null) {
                        throw new IllegalArgumentException("Expected baseName to be null");
                    }

                    if (parameterStart != -1) {
                        throw new IllegalArgumentException("Expected parameter start to be -1");
                    }

                    baseName = signature.substring(0, i);
                    parameterStart = i + 1;
                }
                bracketCount++;
            } else if (c == '>') {
                bracketCount--;
                if (bracketCount < 0) {
                    throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                }

                if (bracketCount == 0) {
                    if (parameterStart < 0) {
                        throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                    }
                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters, literalParameters);
                    }
                }
            } else if (c == ',') {
                if (bracketCount == 1 && !inLiteralParameters) {
                    if (parameterStart < 0) {
                        throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                    }

                    parameters.add(parseTypeSignature(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                } else if (bracketCount == 0 && inLiteralParameters) {
                    if (parameterStart < 0) {
                        throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                    }

                    literalParameters.add(parseLiteral(signature.substring(parameterStart, i)));
                    parameterStart = i + 1;
                }
            } else if (c == '(') {
                if (inLiteralParameters) {
                    throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                }

                inLiteralParameters = true;
                if (bracketCount == 0) {
                    if (baseName == null) {
                        if (!parameters.isEmpty()) {
                            throw new IllegalArgumentException("Expected no parameters");
                        }

                        if (parameterStart != -1) {
                            throw new IllegalArgumentException("Expected parameter start to be -1");
                        }

                        baseName = signature.substring(0, i);
                    }
                    parameterStart = i + 1;
                }
            } else if (c == ')') {
                if (!inLiteralParameters) {
                    throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                }

                inLiteralParameters = false;
                if (bracketCount == 0) {
                    if (i != signature.length() - 1) {
                        throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                    }

                    if (parameterStart < 0) {
                        throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
                    }

                    literalParameters.add(parseLiteral(signature.substring(parameterStart, i)));
                    return new TypeSignature(baseName, parameters, literalParameters);
                }
            }
        }
        throw new IllegalArgumentException(String.format("Bad type signature: '%s'", signature));
    }

    private static Object parseLiteral(final String literal) {
        if (literal.startsWith("'") || literal.endsWith("'")) {
            if (!(literal.startsWith("'") && literal.endsWith("'"))) {
                throw new IllegalArgumentException(String.format("Bad literal: '%s'", literal));
            }
            return literal.substring(1, literal.length() - 1);
        } else {
            return Long.parseLong(literal);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonValue
    public String toString() {
        final StringBuilder typeName = new StringBuilder(base.getType());
        if (!parameters.isEmpty()) {
            typeName.append("<");
            boolean first = true;
            for (TypeSignature parameter : parameters) {
                if (!first) {
                    typeName.append(",");
                }
                first = false;
                typeName.append(parameter.toString());
            }
            typeName.append(">");
        }
        if (!literalParameters.isEmpty()) {
            typeName.append("(");
            boolean first = true;
            for (Object parameter : literalParameters) {
                if (!first) {
                    typeName.append(",");
                }
                first = false;
                if (parameter instanceof String) {
                    typeName.append("'").append(parameter).append("'");
                } else {
                    typeName.append(parameter.toString());
                }
            }
            typeName.append(")");
        }

        return typeName.toString();
    }

}
