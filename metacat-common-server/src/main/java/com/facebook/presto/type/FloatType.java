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
import io.airlift.slice.SizeOf;

/**
 * Float Type.
 */
public final class FloatType extends AbstractFixedWidthType {
    /** Deafult float type. */
    public static final FloatType FLOAT = new FloatType();
    /** String representation. */
    public static final String TYPE = "float";

    private FloatType() {
        super(TypeSignature.parseTypeSignature(TYPE), float.class, SizeOf.SIZE_OF_FLOAT);
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
        return block.getFloat(position, 0);
    }

    @Override
    public boolean equalTo(final Block leftBlock, final int leftPosition, final Block rightBlock,
        final int rightPosition) {
        final float leftValue = leftBlock.getFloat(leftPosition, 0);
        final float rightValue = rightBlock.getFloat(rightPosition, 0);
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
        final float leftValue = leftBlock.getFloat(leftPosition, 0);
        final float rightValue = rightBlock.getFloat(rightPosition, 0);
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(final Block block, final int position, final BlockBuilder blockBuilder) {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeFloat(block.getFloat(position, 0)).closeEntry();
        }
    }

    @Override
    public double getDouble(final Block block, final int position) {
        return block.getFloat(position, 0);
    }

    @Override
    public void writeDouble(final BlockBuilder blockBuilder, final double value) {
        blockBuilder.writeFloat((float) value).closeEntry();
    }
}
