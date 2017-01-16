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

package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.Lists;
import io.airlift.slice.SizeOf;

import java.math.BigDecimal;

/**
 * Decimal type.
 */
public final class DecimalType extends AbstractFixedWidthType {
    /** Default decimal type. */
    public static final DecimalType DECIMAL = new DecimalType(10, 0);
    /** String representation. */
    public static final String TYPE = "decimal";
    private final int precision;
    private final int scale;

    private DecimalType(final int precision, final int scale) {
        super(new TypeSignature(
            TYPE, Lists.newArrayList(),
            Lists.newArrayList((long) precision, (long) scale)), BigDecimal.class, SizeOf.SIZE_OF_DOUBLE);
        if (precision < 0) {
            throw new IllegalArgumentException("Invalid decimal precision " + precision);
        }
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Creates the decimal type.
     * @param precision precision
     * @param scale scale
     * @return DecimalType
     */
    public static DecimalType createDecimalType(final int precision, final int scale) {
        return new DecimalType(precision, scale);
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public boolean isComparable() {
        return true;
    }

    @Override
    public boolean isOrderable() {
        return true;
    }

    @Override
    public Object getObjectValue(final ConnectorSession session, final Block block, final int position) {
        if (block.isNull(position)) {
            return null;
        }
        return block.getDouble(position, 0);
    }

    @Override
    public boolean equalTo(final Block leftBlock, final int leftPosition, final Block rightBlock,
        final int rightPosition) {
        final double leftValue = leftBlock.getDouble(leftPosition, 0);
        final double rightValue = rightBlock.getDouble(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(final Block block, final int position) {
        final long value = block.getLong(position, 0);
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public int compareTo(final Block leftBlock, final int leftPosition, final Block rightBlock,
        final int rightPosition) {
        final double leftValue = leftBlock.getDouble(leftPosition, 0);
        final double rightValue = rightBlock.getDouble(rightPosition, 0);
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(final Block block, final int position, final BlockBuilder blockBuilder) {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeDouble(block.getDouble(position, 0)).closeEntry();
        }
    }

    @Override
    public double getDouble(final Block block, final int position) {
        return block.getDouble(position, 0);
    }

    @Override
    public void writeDouble(final BlockBuilder blockBuilder, final double value) {
        blockBuilder.writeDouble((double) value).closeEntry();
    }
}
