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

import java.util.Objects;

/**
 * A Range of values across the continuous space defined by the types of the Markers.
 */
@EqualsAndHashCode
@ToString
@SuppressWarnings("checkstyle:javadocmethod")
public final class Range {
    @NonNull
    private final Marker low;
    @NonNull
    private final Marker high;

    @JsonCreator
    public Range(
        @JsonProperty("low") final Marker low,
        @JsonProperty("high") final Marker high) {
        Objects.requireNonNull(low, "value is null");
        Objects.requireNonNull(high, "value is null");
        if (!low.getType().equals(high.getType())) {
            throw new IllegalArgumentException(String.format("Marker types do not match: %s vs %s",
                low.getType(), high.getType()));
        }
        if (low.isUpperUnbounded()) {
            throw new IllegalArgumentException("low cannot be upper unbounded");
        }
        if (high.isLowerUnbounded()) {
            throw new IllegalArgumentException("high cannot be lower unbounded");
        }
        if (low.compareTo(high) > 0) {
            throw new IllegalArgumentException("low must be less than or equal to high");
        }
        this.low = low;
        this.high = high;
    }

    /**
     * all range.
     *
     * @param type
     * @return
     */
    public static Range all(final Class<?> type) {
        return new Range(Marker.lowerUnbounded(type), Marker.upperUnbounded(type));
    }

    public static Range greaterThan(final Comparable<?> low) {
        return new Range(Marker.above(low), Marker.upperUnbounded(low.getClass()));
    }

    public static Range greaterThanOrEqual(final Comparable<?> low) {
        return new Range(Marker.exactly(low), Marker.upperUnbounded(low.getClass()));
    }

    public static Range lessThan(final Comparable<?> high) {
        return new Range(Marker.lowerUnbounded(high.getClass()), Marker.below(high));
    }

    public static Range lessThanOrEqual(final Comparable<?> high) {
        return new Range(Marker.lowerUnbounded(high.getClass()), Marker.exactly(high));
    }

    public static Range equal(final Comparable<?> value) {
        return new Range(Marker.exactly(value), Marker.exactly(value));
    }

    public static Range range(final Comparable<?> low, final boolean lowInclusive,
                              final Comparable<?> high, final boolean highInclusive) {
        final Marker lowMarker = lowInclusive ? Marker.exactly(low) : Marker.above(low);
        final Marker highMarker = highInclusive ? Marker.exactly(high) : Marker.below(high);
        return new Range(lowMarker, highMarker);
    }

    @JsonIgnore
    public Class<?> getType() {
        return low.getType();
    }

    @JsonProperty
    public Marker getLow() {
        return low;
    }

    @JsonProperty
    public Marker getHigh() {
        return high;
    }

    @JsonIgnore
    public boolean isSingleValue() {
        return !low.isLowerUnbounded()
            && !high.isUpperUnbounded()
            && low.getBound() == Marker.Bound.EXACTLY
            && high.getBound() == Marker.Bound.EXACTLY
            && low.getValue().equals(high.getValue());
    }

    @JsonIgnore
    public Comparable<?> getSingleValue() {
        if (!isSingleValue()) {
            throw new IllegalStateException("Range does not have just a single value");
        }
        return low.getValue();
    }

    @JsonIgnore
    public boolean isAll() {
        return low.isLowerUnbounded() && high.isUpperUnbounded();
    }

    public boolean includes(final Marker marker) {
        Objects.requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);
        return low.compareTo(marker) <= 0 && high.compareTo(marker) >= 0;
    }

    public boolean contains(final Range other) {
        checkTypeCompatibility(other);
        return this.getLow().compareTo(other.getLow()) <= 0
            && this.getHigh().compareTo(other.getHigh()) >= 0;
    }

    public Range span(final Range other) {
        checkTypeCompatibility(other);
        final Marker lowMarker = Marker.min(low, other.getLow());
        final Marker highMarker = Marker.max(high, other.getHigh());
        return new Range(lowMarker, highMarker);
    }

    public boolean overlaps(final Range other) {
        checkTypeCompatibility(other);
        return this.getLow().compareTo(other.getHigh()) <= 0
            && other.getLow().compareTo(this.getHigh()) <= 0;
    }

    public Range intersect(final Range other) {
        checkTypeCompatibility(other);
        if (!this.overlaps(other)) {
            throw new IllegalArgumentException("Cannot intersect non-overlapping ranges");
        }
        final Marker lowMarker = Marker.max(low, other.getLow());
        final Marker highMarker = Marker.min(high, other.getHigh());
        return new Range(lowMarker, highMarker);
    }

    private void checkTypeCompatibility(final Range range) {
        if (!getType().equals(range.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Range types: %s vs %s",
                getType(), range.getType()));
        }
    }

    private void checkTypeCompatibility(final Marker marker) {
        if (!getType().equals(marker.getType())) {
            throw new IllegalArgumentException(String.format("Marker of %s does not match Range of %s",
                marker.getType(), getType()));
        }
    }
}
