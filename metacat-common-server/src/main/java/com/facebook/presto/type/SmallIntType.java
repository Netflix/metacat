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

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;

/**
 * Created by amajumdar on 6/15/15.
 */
public final class SmallIntType extends AbstractFixedWidthType {
    public static final SmallIntType SMALL_INT = new SmallIntType();
    public static final String TYPE = "smallint";

    private SmallIntType() {
        super(parseTypeSignature(TYPE), int.class, SIZE_OF_SHORT);
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
    public Object getObjectValue(ConnectorSession session, Block block, int position) {
        if (block.isNull(position)) {
            return null;
        }

        return block.getShort(position, 0);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition) {
        short leftValue = leftBlock.getShort(leftPosition, 0);
        short rightValue = rightBlock.getShort(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position) {
        return block.getShort(position, 0);
    }

    @Override
    @SuppressWarnings("SuspiciousNameCombination")
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition) {
        short leftValue = leftBlock.getShort(leftPosition, 0);
        short rightValue = rightBlock.getShort(rightPosition, 0);
        return Short.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder) {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeShort(block.getShort(position, 0)).closeEntry();
        }
    }

    @Override
    public long getLong(Block block, int position) {
        return block.getShort(position, 0);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value) {
        blockBuilder.writeShort((short)value).closeEntry();
    }
}
