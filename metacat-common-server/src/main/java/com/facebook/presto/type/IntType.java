package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

/**
 * Created by amajumdar on 6/15/15.
 */
public final class IntType extends AbstractFixedWidthType {
    public static final IntType INT = new IntType();
    public static final String TYPE = "int";

    private IntType() {
        super(parseTypeSignature(TYPE), int.class, SIZE_OF_INT);
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

        return block.getInt(position, 0);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition) {
        long leftValue = leftBlock.getInt(leftPosition, 0);
        long rightValue = rightBlock.getInt(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position) {
        long value = block.getInt(position, 0);
        return (int) (value ^ (value >>> 32));
    }

    @Override
    @SuppressWarnings("SuspiciousNameCombination")
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition) {
        int leftValue = leftBlock.getInt(leftPosition, 0);
        int rightValue = rightBlock.getInt(rightPosition, 0);
        return Integer.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder) {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
        }
    }

    @Override
    public long getLong(Block block, int position) {
        return block.getInt(position, 0);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value) {
        blockBuilder.writeInt((int)value).closeEntry();
    }
}
