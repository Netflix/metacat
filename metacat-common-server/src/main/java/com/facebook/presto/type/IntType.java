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
 * Integer type.
 */
public final class IntType extends AbstractFixedWidthType {
    /** Default Integer type. */
    public static final IntType INT = new IntType();
    /** String representation. */
    public static final String TYPE = "int";

    private IntType() {
        super(TypeSignature.parseTypeSignature(TYPE), int.class, SizeOf.SIZE_OF_INT);
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

        return block.getInt(position, 0);
    }

    @Override
    public boolean equalTo(final Block leftBlock, final int leftPosition, final Block rightBlock,
        final int rightPosition) {
        final int leftValue = leftBlock.getInt(leftPosition, 0);
        final int rightValue = rightBlock.getInt(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(final Block block, final int position) {
        return block.getInt(position, 0);
    }

    @Override
    @SuppressWarnings("SuspiciousNameCombination")
    public int compareTo(final Block leftBlock, final int leftPosition, final Block rightBlock,
        final int rightPosition) {
        final int leftValue = leftBlock.getInt(leftPosition, 0);
        final int rightValue = rightBlock.getInt(rightPosition, 0);
        return Integer.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(final Block block, final int position, final BlockBuilder blockBuilder) {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
        }
    }

    @Override
    public long getLong(final Block block, final int position) {
        return block.getInt(position, 0);
    }

    @Override
    public void writeLong(final BlockBuilder blockBuilder, final long value) {
        blockBuilder.writeInt((int) value).closeEntry();
    }
}
