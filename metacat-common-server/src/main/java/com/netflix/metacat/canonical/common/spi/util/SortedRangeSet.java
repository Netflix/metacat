package com.netflix.metacat.canonical.common.spi.util;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges. This structure
 * allows iteration across these compacted Ranges in increasing order, as well as other common
 * set-related operation.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@ToString
@SuppressWarnings("checkstyle:javadocmethod")
public final class SortedRangeSet
    implements Iterable<Range> {
    @NonNull
    private final Class<?> type;
    @NonNull
    private final NavigableMap<Marker, Range> lowIndexedRanges;

    public static SortedRangeSet none(final Class<?> type) {
        return copyOf(type, Collections.<Range>emptyList());
    }

    public static SortedRangeSet all(final Class<?> type) {
        return copyOf(type, Arrays.asList(Range.all(type)));
    }

    public static SortedRangeSet singleValue(final Comparable<?> value) {
        return SortedRangeSet.of(Range.equal(value));
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet.
     */
    public static SortedRangeSet of(final Range first, final Range... ranges) {
        final List<Range> rangeList = new ArrayList<>();
        rangeList.add(first);
        rangeList.addAll(Arrays.asList(ranges));
        return copyOf(first.getType(), rangeList);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet.
     */
    public static SortedRangeSet copyOf(final Class<?> type, final Iterable<Range> ranges) {
        return new Builder(type).addAll(ranges).build();
    }

    @JsonCreator
    public static SortedRangeSet copyOf(
        @JsonProperty("type") final Class<?> type,
        @JsonProperty("ranges") final List<Range> ranges) {
        return copyOf(type, (Iterable<Range>) ranges);
    }

    @JsonProperty
    public Class<?> getType() {
        return type;
    }

    @JsonProperty
    public List<Range> getRanges() {
        final ArrayList<Range> ranges = new ArrayList<>();
        ranges.addAll(lowIndexedRanges.values());
        return ranges;
    }

    @JsonIgnore
    public int getRangeCount() {
        return lowIndexedRanges.size();
    }

    @JsonIgnore
    public boolean isNone() {
        return lowIndexedRanges.isEmpty();
    }

    @JsonIgnore
    public boolean isAll() {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isAll();
    }

    @JsonIgnore
    public boolean isSingleValue() {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isSingleValue();
    }

    @JsonIgnore
    public Comparable<?> getSingleValue() {
        if (!isSingleValue()) {
            throw new IllegalStateException("SortedRangeSet does not have just a single value");
        }
        return lowIndexedRanges.values().iterator().next().getSingleValue();
    }

    public boolean includesMarker(final Marker marker) {
        Objects.requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);
        final Map.Entry<Marker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    @JsonIgnore
    public Range getSpan() {
        if (lowIndexedRanges.isEmpty()) {
            throw new IllegalStateException("Can not get span if no ranges exist");
        }
        return lowIndexedRanges.firstEntry().getValue().span(lowIndexedRanges.lastEntry().getValue());
    }

    public boolean overlaps(final SortedRangeSet other) {
        checkTypeCompatibility(other);
        return !this.intersect(other).isNone();
    }

    public boolean contains(final SortedRangeSet other) {
        checkTypeCompatibility(other);
        return this.union(other).equals(this);
    }

    public SortedRangeSet intersect(final SortedRangeSet other) {
        checkTypeCompatibility(other);

        final Builder builder = new Builder(type);

        final Iterator<Range> iter1 = iterator();
        final Iterator<Range> iter2 = other.iterator();

        if (iter1.hasNext() && iter2.hasNext()) {
            Range range1 = iter1.next();
            Range range2 = iter2.next();

            while (true) {
                if (range1.overlaps(range2)) {
                    builder.add(range1.intersect(range2));
                }

                if (range1.getHigh().compareTo(range2.getHigh()) <= 0) {
                    if (!iter1.hasNext()) {
                        break;
                    }
                    range1 = iter1.next();
                } else {
                    if (!iter2.hasNext()) {
                        break;
                    }
                    range2 = iter2.next();
                }
            }
        }

        return builder.build();
    }

    public SortedRangeSet union(final SortedRangeSet other) {
        checkTypeCompatibility(other);
        return new Builder(type)
            .addAll(this)
            .addAll(other)
            .build();
    }

    public static SortedRangeSet union(final Iterable<SortedRangeSet> ranges) {
        final Iterator<SortedRangeSet> iterator = ranges.iterator();
        if (!iterator.hasNext()) {
            throw new IllegalArgumentException("ranges must have at least one element");
        }

        final SortedRangeSet first = iterator.next();
        final Builder builder = new Builder(first.type);
        builder.addAll(first);

        while (iterator.hasNext()) {
            builder.addAll(iterator.next());
        }

        return builder.build();
    }

    public SortedRangeSet complement() {
        final Builder builder = new Builder(type);

        if (lowIndexedRanges.isEmpty()) {
            return builder.add(Range.all(type)).build();
        }

        final Iterator<Range> rangeIterator = lowIndexedRanges.values().iterator();

        final Range firstRange = rangeIterator.next();
        if (!firstRange.getLow().isLowerUnbounded()) {
            builder.add(new Range(Marker.lowerUnbounded(type), firstRange.getLow().lesserAdjacent()));
        }

        Range previousRange = firstRange;
        while (rangeIterator.hasNext()) {
            final Range currentRange = rangeIterator.next();

            final Marker lowMarker = previousRange.getHigh().greaterAdjacent();
            final Marker highMarker = currentRange.getLow().lesserAdjacent();
            builder.add(new Range(lowMarker, highMarker));

            previousRange = currentRange;
        }

        final Range lastRange = previousRange;
        if (!lastRange.getHigh().isUpperUnbounded()) {
            builder.add(new Range(lastRange.getHigh().greaterAdjacent(), Marker.upperUnbounded(type)));
        }

        return builder.build();
    }

    public SortedRangeSet subtract(final SortedRangeSet other) {
        checkTypeCompatibility(other);
        return this.intersect(other.complement());
    }

    private void checkTypeCompatibility(final SortedRangeSet other) {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(String.format("Mismatched SortedRangeSet types: %s vs %s",
                getType(), other.getType()));
        }
    }

    private void checkTypeCompatibility(final Marker marker) {
        if (!getType().equals(marker.getType())) {
            throw new IllegalStateException(String.format("Marker of %s does not match SortedRangeSet of %s",
                marker.getType(), getType()));
        }
    }

    @Override
    public Iterator<Range> iterator() {
        return Collections.unmodifiableCollection(lowIndexedRanges.values()).iterator();
    }

    public static Builder builder(final Class<?> type) {
        return new Builder(type);
    }

    /**
     * builder.
     */
    public static class Builder {
        private static final Comparator<Range> LOW_MARKER_COMPARATOR = new Comparator<Range>() {
            @Override
            public int compare(final Range o1, final Range o2) {
                return o1.getLow().compareTo(o2.getLow());
            }
        };

        private final Class<?> type;
        private final List<Range> ranges = new ArrayList<>();

        public Builder(final Class<?> type) {
            this.type = Objects.requireNonNull(type, "type is null");
        }

        public Builder add(final Range range) {
            if (!type.equals(range.getType())) {
                throw new IllegalArgumentException(String.format("Range type %s does not match builder type %s",
                    range.getType(), type));
            }

            ranges.add(range);
            return this;
        }

        public Builder addAll(final Iterable<Range> rangeIt) {
            for (Range range : rangeIt) {
                add(range);
            }
            return this;
        }

        public SortedRangeSet build() {
            Collections.sort(ranges, LOW_MARKER_COMPARATOR);

            final NavigableMap<Marker, Range> result = new TreeMap<>();

            Range current = null;
            for (Range next : ranges) {
                if (current == null) {
                    current = next;
                    continue;
                }

                if (current.overlaps(next) || current.getHigh().isAdjacent(next.getLow())) {
                    current = current.span(next);
                } else {
                    result.put(current.getLow(), current);
                    current = next;
                }
            }

            if (current != null) {
                result.put(current.getLow(), current);
            }

            return new SortedRangeSet(type, result);
        }
    }
}
