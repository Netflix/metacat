package com.netflix.metacat.canonical.common.spi.util;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Defines the possible values of a single variable in terms of its valid scalar ranges and nullability.
 * <p>
 * For example:
 * Domain.none() => no scalar values allowed, NULL not allowed
 * Domain.all() => all scalar values allowed, NULL allowed
 * Domain.onlyNull() => no scalar values allowed, NULL allowed
 * Domain.notNull() => all scalar values allowed, NULL not allowed
 */
@ToString
@EqualsAndHashCode
public final class Domain {
    private final SortedRangeSet ranges;
    private final boolean nullAllowed;

    /**
     * constructor.
     *
     * @param ranges      range
     * @param nullAllowed nullAllowed flag
     */
    @JsonCreator
    public Domain(
        @JsonProperty("ranges") final SortedRangeSet ranges,
        @JsonProperty("nullAllowed") final boolean nullAllowed) {
        this.ranges = Objects.requireNonNull(ranges, "ranges is null");
        this.nullAllowed = nullAllowed;
        if (ranges.getType().isPrimitive()) {
            throw new IllegalArgumentException("Primitive types not supported: " + ranges.getType());
        }
    }

    /**
     * create domain.
     *
     * @param ranges      ranges
     * @param nullAllowed null allowed flag
     * @return domain
     */
    public static Domain create(final SortedRangeSet ranges, final boolean nullAllowed) {
        return new Domain(ranges, nullAllowed);
    }

    /**
     * create None domain.
     *
     * @param type type
     * @return domain
     */
    public static Domain none(final Class<?> type) {
        return new Domain(SortedRangeSet.none(type), false);
    }

    /**
     * all domain.
     *
     * @param type type
     * @return domain.
     */
    public static Domain all(final Class<?> type) {
        return new Domain(SortedRangeSet.of(Range.all(type)), true);
    }

    /**
     * only null domain.
     *
     * @param type type
     * @return domain
     */
    public static Domain onlyNull(final Class<?> type) {
        return new Domain(SortedRangeSet.none(type), true);
    }

    /**
     * not null domain.
     *
     * @param type type
     * @return domain
     */
    public static Domain notNull(final Class<?> type) {
        return new Domain(SortedRangeSet.all(type), false);
    }

    /**
     * single value domain.
     *
     * @param value value
     * @return domain
     */
    public static Domain singleValue(final Comparable<?> value) {
        return new Domain(SortedRangeSet.of(Range.equal(value)), false);
    }

    @JsonIgnore
    public Class<?> getType() {
        return ranges.getType();
    }

    /**
     * Returns a SortedRangeSet to represent the set of scalar values that are allowed in this Domain.
     * An empty (a.k.a. "none") SortedRangeSet indicates that no scalar values are allowed.
     *
     * @return range
     */
    @JsonProperty
    public SortedRangeSet getRanges() {
        return ranges;
    }

    @JsonProperty
    public boolean isNullAllowed() {
        return nullAllowed;
    }

    @JsonIgnore
    public boolean isNone() {
        return equals(Domain.none(getType()));
    }

    @JsonIgnore
    public boolean isAll() {
        return equals(Domain.all(getType()));
    }

    @JsonIgnore
    public boolean isSingleValue() {
        return !nullAllowed && ranges.isSingleValue();
    }

    /**
     * isNullableSingleValue.
     *
     * @return boolean
     */
    @JsonIgnore
    public boolean isNullableSingleValue() {
        if (nullAllowed) {
            return ranges.isNone();
        } else {
            return ranges.isSingleValue();
        }
    }

    /**
     * isOnlyNull.
     *
     * @return boolean
     */
    @JsonIgnore
    public boolean isOnlyNull() {
        return equals(onlyNull(getType()));
    }

    /**
     * getSingleValue.
     *
     * @return value
     */
    @JsonIgnore
    public Comparable<?> getSingleValue() {
        if (!isSingleValue()) {
            throw new IllegalStateException("Domain is not a single value");
        }
        return ranges.getSingleValue();
    }

    /**
     * getNullableSingleValue.
     *
     * @return value
     */
    @JsonIgnore
    public Comparable<?> getNullableSingleValue() {
        if (!isNullableSingleValue()) {
            throw new IllegalStateException("Domain is not a single value");
        }

        if (nullAllowed) {
            return null;
        } else {
            return ranges.getSingleValue();
        }
    }

    /**
     * includesValue.
     *
     * @param value value
     * @return boolean
     */
    public boolean includesValue(final Comparable<?> value) {
        return value == null ? nullAllowed : ranges.includesMarker(Marker.exactly(value));
    }

    /**
     * overlaps.
     *
     * @param other other
     * @return boolean
     */
    public boolean overlaps(final Domain other) {
        checkTypeCompatibility(other);
        return !this.intersect(other).isNone();
    }

    /**
     * contains.
     *
     * @param other other
     * @return boolean
     */
    public boolean contains(final Domain other) {
        checkTypeCompatibility(other);
        return this.union(other).equals(this);
    }

    /**
     * intersect.
     *
     * @param other other
     * @return domain
     */
    public Domain intersect(final Domain other) {
        checkTypeCompatibility(other);
        final SortedRangeSet intersectedRanges = this.getRanges().intersect(other.getRanges());
        return new Domain(intersectedRanges, this.isNullAllowed() && other.isNullAllowed());
    }

    /**
     * union.
     *
     * @param other other
     * @return domain.
     */
    public Domain union(final Domain other) {
        checkTypeCompatibility(other);
        final SortedRangeSet unionRanges = this.getRanges().union(other.getRanges());
        return new Domain(unionRanges, this.isNullAllowed() || other.isNullAllowed());
    }

    /**
     * union.
     *
     * @param domains domains
     * @return domain.
     */
    public static Domain union(final List<Domain> domains) {
        if (domains.size() == 1) {
            return domains.get(0);
        }

        boolean nullAllowed = false;
        final List<SortedRangeSet> ranges = new ArrayList<>();
        for (Domain domain : domains) {
            ranges.add(domain.getRanges());
            nullAllowed = nullAllowed || domain.nullAllowed;
        }

        return new Domain(SortedRangeSet.union(ranges), nullAllowed);
    }

    /**
     * complement.
     *
     * @return domain.
     */
    public Domain complement() {
        return new Domain(ranges.complement(), !nullAllowed);
    }

    /**
     * subtract.
     *
     * @param other other
     * @return domain
     */
    public Domain subtract(final Domain other) {
        checkTypeCompatibility(other);
        final SortedRangeSet subtractedRanges = this.getRanges().subtract(other.getRanges());
        return new Domain(subtractedRanges, this.isNullAllowed() && !other.isNullAllowed());
    }

    private void checkTypeCompatibility(final Domain domain) {
        if (!getType().equals(domain.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Domain types: %s vs %s",
                getType(), domain.getType()));
        }
    }

}
