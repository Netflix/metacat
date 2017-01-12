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

package com.netflix.metacat.canonical.common.spi.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * A point on the continuous space defined by the specified type.
 * Each point may be just below, exact, or just above the specified value according to the Bound.
 */
@EqualsAndHashCode
@ToString
public final class Marker
    implements Comparable<Marker> {
    /**
     * bound class.
     */
    public enum Bound {
        /**
         * lower than the value, but infinitesimally close to the value.
         */
        BELOW,
        /**
         * exactly the value.
         */
        EXACTLY,
        /**
         * higher than the value, but infinitesimally close to the value.
         */
        ABOVE
    }

    private final Class<?> type;
    private final Comparable<?> value;
    private final Bound bound;

    /**
     * LOWER UNBOUNDED is specified with a null value and a ABOVE bound.
     * UPPER UNBOUNDED is specified with a null value and a BELOW bound
     */
    private Marker(final Class<?> type, final Comparable<?> value, final Bound bound) {
        Objects.requireNonNull(type, "type is null");
        Objects.requireNonNull(bound, "bound is null");
        if (value != null && !verifySelfComparable(type)) {
            // Condition "value != null" enables LOWER UNBOUNDED and UPPER UNBOUNDED values for non-comparable types.

            // !!!HACK ALERT!!!
            // It is planned that we will support non-comparable type in TupleDomain. Until that day comes, the
            // `value != null` part of this if condition is a HACK so that Block can be used as a native container
            // type. Consider this a hack and don't rely on it because plans CAN change.

            // Notes: assuming type and value match, one would quickly conclude that type implements some sort of
            // Comparable. This conclusion is WRONG. For example, type=Object.class and value=null.
            throw new IllegalArgumentException("type must be comparable to itself: " + type);
        }
        if (value == null && bound == Bound.EXACTLY) {
            throw new IllegalArgumentException("Can not be equal to unbounded");
        }
        if (value != null && !type.isInstance(value)) {
            throw new IllegalArgumentException(String.format("value (%s) must be of specified type (%s)", value, type));
        }
        this.type = type;
        this.value = value;
        this.bound = bound;
    }

    /**
     * constructor.
     *
     * @param value value
     * @param bound bound
     */
    @JsonCreator
    public Marker(
        @JsonProperty("value") final SerializableNativeValue value,
        @JsonProperty("bound") final Bound bound) {
        this(value.getType(), value.getValue(), bound);
    }

    private static boolean verifySelfComparable(final Class<?> type) {
        for (Type interfaceType : type.getGenericInterfaces()) {
            if (interfaceType instanceof ParameterizedType) {
                final ParameterizedType parameterizedType = (ParameterizedType) interfaceType;
                if (parameterizedType.getRawType().equals(Comparable.class)) {
                    final Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                    final Type typeArgument = actualTypeArguments[0];
                    if (typeArgument.equals(type)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * upperUnbounded.
     *
     * @param type type
     * @return marker
     */
    public static Marker upperUnbounded(final Class<?> type) {
        return new Marker(type, null, Bound.BELOW);
    }

    /**
     * lowerunbounded.
     *
     * @param type type
     * @return marker
     */
    public static Marker lowerUnbounded(final Class<?> type) {
        return new Marker(type, null, Bound.ABOVE);
    }

    /**
     * above.
     *
     * @param value value
     * @return marker
     */
    public static Marker above(final Comparable<?> value) {
        Objects.requireNonNull(value, "value is null");
        return new Marker(value.getClass(), value, Bound.ABOVE);
    }

    /**
     * exactly.
     *
     * @param value value
     * @return marker
     */
    public static Marker exactly(@NonNull final Comparable<?> value) {
        return new Marker(value.getClass(), value, Bound.EXACTLY);
    }

    /**
     * below.
     *
     * @param value value
     * @return marker
     */
    public static Marker below(@NonNull final Comparable<?> value) {
        return new Marker(value.getClass(), value, Bound.BELOW);
    }

    @JsonIgnore
    public Class<?> getType() {
        return type;
    }

    /**
     * getvalue.
     *
     * @return comparable
     */
    @JsonIgnore
    public Comparable<?> getValue() {
        if (value == null) {
            throw new IllegalStateException("Can not get value for unbounded");
        }
        return value;
    }

    @JsonProperty("value")
    public SerializableNativeValue getSerializableNativeValue() {
        return new SerializableNativeValue(type, value);
    }

    @JsonProperty
    public Bound getBound() {
        return bound;
    }

    @JsonIgnore
    public boolean isUpperUnbounded() {
        return value == null && bound == Bound.BELOW;
    }

    @JsonIgnore
    public boolean isLowerUnbounded() {
        return value == null && bound == Bound.ABOVE;
    }

    private void checkTypeCompatibility(final Marker marker) {
        if (!type.equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Marker types: %s vs %s",
                type, marker.getType()));
        }
    }

    /**
     * Adjacency is defined by two Markers being infinitesimally close to each other.
     * This means they must share the same value and have adjacent Bounds.
     *
     * @param other other
     * @return boolean
     */
    public boolean isAdjacent(final Marker other) {
        checkTypeCompatibility(other);
        if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
            return false;
        }
        if (compare(value, other.value) != 0) {
            return false;
        }
        return (bound == Bound.EXACTLY && other.bound != Bound.EXACTLY)
            || (bound != Bound.EXACTLY && other.bound == Bound.EXACTLY);
    }

    /**
     * greater adj.
     * @return marker
     */
    public Marker greaterAdjacent() {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new Marker(type, value, Bound.EXACTLY);
            case EXACTLY:
                return new Marker(type, value, Bound.ABOVE);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    /**
     * less adjacent.
     * @return marker
     */
    public Marker lesserAdjacent() {
        if (value == null) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker(type, value, Bound.BELOW);
            case ABOVE:
                return new Marker(type, value, Bound.EXACTLY);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    @Override
    public int compareTo(final Marker o) {
        checkTypeCompatibility(o);
        if (isUpperUnbounded()) {
            return o.isUpperUnbounded() ? 0 : 1;
        }
        if (isLowerUnbounded()) {
            return o.isLowerUnbounded() ? 0 : -1;
        }
        if (o.isUpperUnbounded()) {
            return -1;
        }
        if (o.isLowerUnbounded()) {
            return 1;
        }
        // INVARIANT: value and o.value not null

        final int compare = compare(value, o.value);
        if (compare == 0) {
            if (bound == o.bound) {
                return 0;
            }
            if (bound == Bound.BELOW) {
                return -1;
            }
            if (bound == Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o.bound == Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(final Comparable<?> value1, final Comparable<?> value2) {
        // This is terrible, but it should be safe as we have checked the compatibility in the constructor
        return ((Comparable) value1).compareTo(value2);
    }

    /**
     * min.
     *
     * @param marker1 marker1
     * @param marker2 marker2
     * @return marker
     */
    public static Marker min(final Marker marker1, final Marker marker2) {
        return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
    }

    /**
     * max.
     *
     * @param marker1 marker1
     * @param marker2 marker2
     * @return marker
     */
    public static Marker max(final Marker marker1, final Marker marker2) {
        return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
    }
}
