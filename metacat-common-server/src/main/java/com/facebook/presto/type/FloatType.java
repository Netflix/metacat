package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;

public final class FloatType extends AbstractFixedWidthType
{
    public static final FloatType FLOAT = new FloatType();
    public static final String TYPE = "float";

    private FloatType()
    {
        super(parseTypeSignature(TYPE), float.class, SIZE_OF_FLOAT);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return block.getFloat(position, 0);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        float leftValue = leftBlock.getFloat(leftPosition, 0);
        float rightValue = rightBlock.getFloat(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position)
    {
        long value = block.getLong(position, 0);
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        float leftValue = leftBlock.getFloat(leftPosition, 0);
        float rightValue = rightBlock.getFloat(rightPosition, 0);
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeFloat(block.getFloat(position, 0)).closeEntry();
        }
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return block.getFloat(position, 0);
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        blockBuilder.writeFloat((float)value).closeEntry();
    }
}