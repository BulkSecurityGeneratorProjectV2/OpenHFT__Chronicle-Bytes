/*
 * Copyright (c) 2016-2022 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.bytes;

import net.openhft.chronicle.assertions.AssertUtil;
import net.openhft.chronicle.bytes.internal.*;
import net.openhft.chronicle.bytes.internal.migration.HashCodeEqualsUtil;
import net.openhft.chronicle.bytes.util.DecoratedBufferOverflowException;
import net.openhft.chronicle.bytes.util.DecoratedBufferUnderflowException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.UnsafeMemory;
import net.openhft.chronicle.core.annotation.NonNegative;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.core.io.UnsafeText;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static net.openhft.chronicle.core.util.Ints.requireNonNegative;
import static net.openhft.chronicle.core.util.Longs.requireNonNegative;
import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * Abstract representation of Bytes.
 *
 * @param <U> Underlying type
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractBytes<U>
        extends AbstractReferenceCounted
        implements Bytes<U>, HasUncheckedRandomDataInput {
    private static final byte[] EMPTY_ARRAY = new byte[0];
    private static final boolean BYTES_BOUNDS_UNCHECKED = Jvm.getBoolean("bytes.bounds.unchecked", false);

    // used for debugging
    @UsedViaReflection
    private final String name;
    private final UncheckedRandomDataInput uncheckedRandomDataInput = new UncheckedRandomDataInputHolder();
    @NotNull
    protected BytesStore<Bytes<U>, U> bytesStore;
    protected long readPosition;
    protected long writePosition;
    protected long writeLimit;
    protected boolean isPresent;
    private int lastDecimalPlaces = 0;
    private boolean lenient = false;
    private boolean lastNumberHadDigits = false;

    AbstractBytes(@NotNull BytesStore<Bytes<U>, U> bytesStore, @NonNegative long writePosition, @NonNegative long writeLimit)
            throws IllegalStateException {
        this(bytesStore, writePosition, writeLimit, "");
    }

    AbstractBytes(@NotNull BytesStore<Bytes<U>, U> bytesStore, @NonNegative long writePosition, @NonNegative long writeLimit, String name)
            throws IllegalStateException {
        super(bytesStore.isDirectMemory());
        this.bytesStore = requireNonNull(bytesStore);
        readPosition = bytesStore.readPosition();
        this.writePosition = writePosition;
        this.writeLimit = writeLimit;
        // used for debugging
        this.name = name;
        bytesStore.reserve(this);
    }

    @Override
    public boolean isDirectMemory() {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return bytesStore.isDirectMemory();
    }

    @Override
    public boolean canReadDirect(@NonNegative long length) {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long remaining = writePosition() - readPosition;
        return bytesStore.isDirectMemory() && remaining >= length;
    }

    @Override
    public void move(@NonNegative long from, @NonNegative long to, @NonNegative long length)
            throws BufferUnderflowException, IllegalStateException, ArithmeticException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long start = start();
        bytesStore.move(from - start, to - start, length);
    }

    @NotNull
    @Override
    public Bytes<U> compact()
            throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long start = start();
        long readRemaining = readRemaining();
        try {
            if ((readRemaining > 0) && (start < readPosition)) {
                bytesStore.move(readPosition, start, readRemaining);
                readPosition = start;
                uncheckedWritePosition(readPosition + readRemaining);
            }
            return this;
        } catch (BufferUnderflowException | ArithmeticException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    @NotNull
    public Bytes<U> clear()
            throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long start = start();
        readPosition = start;
        uncheckedWritePosition(start);
        writeLimit = capacity();
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> clearAndPad(@NonNegative long length)
            throws BufferOverflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        final long start = start();
        if ((start + length) > capacity()) {
            throw newBOERange(start, length, "clearAndPad failed. Start: %d + length: %d > capacity: %d", capacity());
        }
        long l = start + length;
        readPosition = l;
        uncheckedWritePosition(l);
        writeLimit = capacity();
        return this;
    }

    @Override
    public long readLimit() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return writePosition();
    }

    @Override
    public long writeLimit() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return writeLimit;
    }

    @Override
    public @NonNegative long realCapacity() {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return bytesStore.capacity();
    }

    @Override
    public boolean canWriteDirect(@NonNegative long count) {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return isDirectMemory() &&
                Math.min(writeLimit, bytesStore.realCapacity())
                        >= count + writePosition();
    }

    @NonNegative
    @Override
    public long capacity() {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return bytesStore.capacity();
    }

    @Nullable
    @Override
    public U underlyingObject() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore.underlyingObject();
    }

    @NonNegative
    @Override
    public long start() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore.start();
    }

    @Override
    public @NonNegative long readPosition() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return readPosition;
    }

    @Override
    public @NonNegative long writePosition() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return writePosition;
    }

    @Override
    public boolean compareAndSwapInt(@NonNegative long offset, int expected, int value)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 4);
        return bytesStore.compareAndSwapInt(offset, expected, value);
    }

    @Override
    public void testAndSetInt(@NonNegative long offset, int expected, int value)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 4);
        bytesStore.testAndSetInt(offset, expected, value);
    }

    @Override
    public boolean compareAndSwapLong(@NonNegative long offset, long expected, long value)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 8);
        return bytesStore.compareAndSwapLong(offset, expected, value);
    }

    @Override
    public @NotNull AbstractBytes<U> append(double d)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        boolean fits = canWriteDirect(380);
        double ad = Math.abs(d);
        if (ad < 1e-18) {
            append(Double.toString(d));
            return this;
        }
        if (!fits) {
            fits = 1e-6 <= ad && ad < 1e20 && canWriteDirect(24);
        }
        if (fits) {
            long address = addressForWrite(writePosition());
            long address2 = UnsafeText.appendDouble(address, d);
            writeSkip(address2 - address);
            return this;
        }
        BytesInternal.append(this, d);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> readPosition(@NonNegative long position)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (position < start()) {
            throw new DecoratedBufferUnderflowException(String.format("readPosition failed. Position: %d < start: %d", position, start()));
        }
        if (position > readLimit()) {
            throw new DecoratedBufferUnderflowException(
                    String.format("readPosition failed. Position: %d > readLimit: %d", position, readLimit()));
        }
        this.readPosition = position;
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> readLimit(@NonNegative long limit)
            throws BufferUnderflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (limit < start())
            throw limitLessThanStart(limit);

        if (limit > writeLimit())
            throw limitGreaterThanWriteLimit(limit);

        uncheckedWritePosition(limit);
        return this;
    }

    private DecoratedBufferUnderflowException limitGreaterThanWriteLimit(long limit) {
        return new DecoratedBufferUnderflowException(String.format("readLimit failed. Limit: %d > writeLimit: %d", limit, writeLimit()));
    }

    private DecoratedBufferUnderflowException limitLessThanStart(long limit) {
        return new DecoratedBufferUnderflowException(String.format("readLimit failed. Limit: %d < start: %d", limit, start()));
    }

    @NotNull
    @Override
    public Bytes<U> writePosition(@NonNegative long position)
            throws BufferOverflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (position > writeLimit())
            throw writePositionTooLarge(position);

        if (position < start())
            throw writePositionTooSmall(position);

        if (position < readPosition())
            this.readPosition = position;
        ensureCapacity(position);
        uncheckedWritePosition(position);
        return this;
    }

    @NotNull
    private DecoratedBufferOverflowException writePositionTooSmall(@NonNegative long position) {
        return new DecoratedBufferOverflowException(String.format("writePosition failed. Position: %d < start: %d", position, start()));
    }

    private DecoratedBufferOverflowException writePositionTooLarge(@NonNegative long position) {
        return new DecoratedBufferOverflowException(
                String.format("writePosition failed. Position: %d > writeLimit: %d", position, writeLimit()));
    }

    @NotNull
    @Override
    public Bytes<U> readSkip(long bytesToSkip)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (lenient) {
            bytesToSkip = Math.min(bytesToSkip, readRemaining());
        }
        readOffsetPositionMoved(bytesToSkip);
        return this;
    }

    @Override
    public void uncheckedReadSkipOne() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        readPosition++;
    }

    @Override
    public void uncheckedReadSkipBackOne() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        readPosition--;
    }

    @NotNull
    @Override
    public Bytes<U> writeSkip(long bytesToSkip)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        final long writePos = writePosition();
        writeCheckOffset(writePos, bytesToSkip);
        uncheckedWritePosition(writePos + bytesToSkip);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeLimit(@NonNegative long limit)
            throws BufferOverflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (limit < start()) {
            throw writeLimitTooSmall(limit);
        }
        long capacity = capacity();
        if (limit > capacity) {
            throw writeLimitTooBig(limit, capacity);
        }
        this.writeLimit = limit;
        return this;
    }

    @NotNull
    private DecoratedBufferOverflowException writeLimitTooBig(@NonNegative long limit, @NonNegative long capacity) {
        return new DecoratedBufferOverflowException(String.format("writeLimit failed. Limit: %d > capacity: %d", limit, capacity));
    }

    @NotNull
    private DecoratedBufferOverflowException writeLimitTooSmall(@NonNegative long limit) {
        return new DecoratedBufferOverflowException(String.format("writeLimit failed. Limit: %d < start: %d", limit, start()));
    }

    @Override
    protected void performRelease() {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        try {
            this.bytesStore.release(this);
        } catch (IllegalStateException e) {
            Jvm.warn().on(getClass(), e);
        }
    }

    @Override
    public int readUnsignedByte()
            throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(1);
            return bytesStore.readUnsignedByte(offset);

        } catch (BufferUnderflowException e) {
            return -1;
        }
    }

    @Override
    public int readUnsignedByte(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return readByte(offset) & 0xFF;
    }

    @Override
    public int uncheckedReadUnsignedByte() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            int unsignedByte = bytesStore.readUnsignedByte(readPosition);
            readPosition++;
            return unsignedByte;
        } catch (BufferUnderflowException | IllegalStateException e) {
            return -1;
        }
    }

    @Override
    public byte readByte()
            throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(1);
            return bytesStore.readByte(offset);

        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public int peekUnsignedByte()
            throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            return readPosition >= writePosition() ? -1 : bytesStore.readUnsignedByte(readPosition);
        } catch (BufferUnderflowException e) {
            return -1;
        }
    }

    @Override
    public short readShort()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(2);
            return bytesStore.readShort(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public int readInt()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(4);
            return bytesStore.readInt(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public byte readVolatileByte(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 1, true);
        return bytesStore.readVolatileByte(offset);
    }

    @Override
    public short readVolatileShort(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 2, true);
        return bytesStore.readVolatileShort(offset);
    }

    @Override
    public int readVolatileInt(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 4, true);
        return bytesStore.readVolatileInt(offset);
    }

    @Override
    public long readVolatileLong(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 8, true);
        return bytesStore.readVolatileLong(offset);
    }

    @Override
    public long readLong()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(8);
            return bytesStore.readLong(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public float readFloat()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(4);
            return bytesStore.readFloat(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public double readDouble()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(8);
            return bytesStore.readDouble(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public int readVolatileInt()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(4);
            return bytesStore.readVolatileInt(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public long readVolatileLong()
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        try {
            long offset = readOffsetPositionMoved(8);
            return bytesStore.readVolatileLong(offset);
        } catch (BufferUnderflowException e) {
            if (lenient) {
                return 0;
            }
            throw e;
        }
    }

    protected long readOffsetPositionMoved(@NonNegative long adding)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = readPosition;
        readCheckOffset(readPosition, Math.toIntExact(adding), false);
        readPosition += adding;
        assert readPosition <= readLimit();
        return offset;
    }

    @NotNull
    @Override
    public Bytes<U> writeByte(@NonNegative long offset, byte i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 1);
        bytesStore.writeByte(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeShort(@NonNegative long offset, short i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 2);
        bytesStore.writeShort(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeInt(@NonNegative long offset, int i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 4);
        bytesStore.writeInt(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeOrderedInt(@NonNegative long offset, int i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 4);
        bytesStore.writeOrderedInt(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeLong(@NonNegative long offset, long i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 8);
        bytesStore.writeLong(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeOrderedLong(@NonNegative long offset, long i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 8);
        bytesStore.writeOrderedLong(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeFloat(@NonNegative long offset, float d)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 4);
        bytesStore.writeFloat(offset, d);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeDouble(@NonNegative long offset, double d)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 8);
        bytesStore.writeDouble(offset, d);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeVolatileByte(@NonNegative long offset, byte i8)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 1);
        bytesStore.writeVolatileByte(offset, i8);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeVolatileShort(@NonNegative long offset, short i16)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 2);
        bytesStore.writeVolatileShort(offset, i16);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeVolatileInt(@NonNegative long offset, int i32)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 4);
        bytesStore.writeVolatileInt(offset, i32);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeVolatileLong(@NonNegative long offset, long i64)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        writeCheckOffset(offset, 8);
        bytesStore.writeVolatileLong(offset, i64);
        return this;
    }

    @Override
    @NotNull
    public Bytes<U> write(@NotNull final RandomDataInput bytes)
            throws IllegalStateException, BufferOverflowException {
        assert bytes != this : "you should not write to yourself !";
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);

        try {
            return write(bytes, bytes.readPosition(), Math.min(writeLimit() - writePosition(), bytes.readRemaining()));
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    @NotNull
    public Bytes<U> write(@NonNegative long offsetInRDO,
                          final byte[] byteArray,
                          @NonNegative int offset,
                          @NonNegative final int length) throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        requireNonNegative(offsetInRDO);
        requireNonNull(byteArray);
        requireNonNegative(offset);
        requireNonNegative(length);
        long remaining = length;
        while (remaining > 0) {
            int copy = (int) Math.min(remaining, safeCopySize()); // copy 64 KB at a time.
            writeCheckOffset(offsetInRDO, copy);
            bytesStore.write(offsetInRDO, byteArray, offset, copy);
            offsetInRDO += copy;
            offset += copy;
            remaining -= copy;
        }
        return this;
    }

    @Override
    public void write(@NonNegative long offsetInRDO, @NotNull ByteBuffer bytes, @NonNegative int offset, @NonNegative int length)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        requireNonNull(bytes);
        if (this.bytesStore.inside(offsetInRDO, length)) {
            writeCheckOffset(offsetInRDO, length);
            bytesStore.write(offsetInRDO, bytes, offset, length);
        } else if (bytes.remaining() <= writeRemaining()) {
            // bounds check
            bytes.get(offset + length - 1);

            int i = 0;
            if (bytes.order() == ByteOrder.nativeOrder()) {
                for (; i < length - 7; i += 8)
                    writeLong(offsetInRDO + i, bytes.getLong(offset + i));
            } else {
                for (; i < length - 7; i += 8)
                    writeLong(offsetInRDO + i, Long.reverseBytes(bytes.getLong(offset + i)));
            }
            for (; i < length; i++)
                writeByte(offsetInRDO + i, bytes.get(offset + i));
        } else {
            throw new DecoratedBufferOverflowException("Unable to write " + length + " with " + writeRemaining() + " remaining");
        }
    }

    @Override
    @NotNull
    public Bytes<U> write(@NonNegative long writeOffset, @NotNull RandomDataInput bytes, @NonNegative long readOffset, @NonNegative long length)
            throws BufferOverflowException, BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        requireNonNegative(writeOffset);
        ReferenceCountedUtil.throwExceptionIfReleased(bytes);
        requireNonNegative(readOffset);
        requireNonNegative(length);
        throwExceptionIfReleased();
        long remaining = length;
        while (remaining > 0) {
            int copy = (int) Math.min(remaining, safeCopySize()); // copy 64 KB at a time.
            writeCheckOffset(writeOffset, copy);
            bytesStore.write(writeOffset, bytes, readOffset, copy);
            writeOffset += copy;
            readOffset += copy;
            remaining -= copy;
        }
        return this;
    }

    @Override
    public @NotNull Bytes<U> write8bit(@NotNull String text, @NonNegative int start, @NonNegative int length) throws BufferOverflowException, IndexOutOfBoundsException, ArithmeticException, IllegalStateException, BufferUnderflowException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        requireNonNull(text); // This needs to be checked or else the JVM might crash
        final long toWriteLength = UnsafeMemory.INSTANCE.stopBitLength(length) + (long) length;
        final long position = writeOffsetPositionMoved(toWriteLength, 0);
        bytesStore.write8bit(position, text, start, length);
        uncheckedWritePosition(writePosition() + toWriteLength);
        return this;
    }

    public @NotNull Bytes<U> write8bit(@Nullable BytesStore bs) throws BufferOverflowException, IllegalStateException, BufferUnderflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (bs == null) {
            BytesInternal.writeStopBitNeg1(this);
            return this;
        }
        long readRemaining = bs.readRemaining();
        long toWriteLength = UnsafeMemory.INSTANCE.stopBitLength(readRemaining) + readRemaining;
        long position = writeOffsetPositionMoved(toWriteLength, 0);
        bytesStore.write8bit(position, bs);
        uncheckedWritePosition(writePosition() + toWriteLength);
        return this;
    }

    @Override
    public long write8bit(@NonNegative long position, @NotNull BytesStore bs) {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return bytesStore.write8bit(position, bs);
    }

    @Override
    public long write8bit(@NonNegative long position, @NotNull String s, @NonNegative int start, @NonNegative int length) {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return bytesStore.write8bit(position, s, start, length);
    }

    protected void writeCheckOffset(@NonNegative long offset, @NonNegative long adding)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        if (BYTES_BOUNDS_UNCHECKED)
            return;
        writeCheckOffset0(offset, adding);
    }

    private void writeCheckOffset0(@NonNegative long offset, @NonNegative long adding)
            throws DecoratedBufferOverflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        final long start = start();
        if (offset < start || offset + adding < start) {
            throw newBOELower(offset);
        }
        if ((offset + adding) > writeLimit()) {
            throw newBOERange(offset, adding, "writeCheckOffset failed. Offset: %d + adding %d> writeLimit: %d", writeLimit());
        }
    }

    @NotNull
    private DecoratedBufferOverflowException newBOERange(@NonNegative long offset, long adding, String msg, @NonNegative long limit) {
        return new DecoratedBufferOverflowException(
                String.format(msg, offset, adding, limit));
    }

    @NotNull
    private DecoratedBufferOverflowException newBOELower(@NonNegative long offset) {
        return new DecoratedBufferOverflowException(String.format("writeCheckOffset failed. Offset: %d < start: %d", offset, start()));
    }

    @Override
    public byte readByte(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 1, true);
        return bytesStore.readByte(offset);
    }

    @Override
    public int peekUnsignedByte(@NonNegative long offset)
            throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return offset < start() || readLimit() <= offset ? -1 : bytesStore.peekUnsignedByte(offset);
    }

    @Override
    public short readShort(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 2, true);
        return bytesStore.readShort(offset);
    }

    @Override
    public int readInt(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 4, true);
        return bytesStore.readInt(offset);
    }

    @Override
    public long readLong(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 8, true);
        return bytesStore.readLong(offset);
    }

    @Override
    public float readFloat(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 4, true);
        return bytesStore.readFloat(offset);
    }

    @Override
    public double readDouble(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        readCheckOffset(offset, 8, true);
        return bytesStore.readDouble(offset);
    }

    protected void readCheckOffset(@NonNegative long offset, long adding, boolean given)
            throws BufferUnderflowException, IllegalStateException {
        if (BYTES_BOUNDS_UNCHECKED)
            return;
        readCheckOffset0(offset, adding, given);
    }

    private void readCheckOffset0(@NonNegative long offset, long adding, boolean given)
            throws DecoratedBufferUnderflowException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        if (offset < start()) {
            throw newBOEReadLower(offset);
        }
        long limit0 = given ? writeLimit() : readLimit();
        if ((offset + adding) > limit0) {
            throw newBOEReadUpper(offset, adding, given);
        }
    }

    @NotNull
    private DecoratedBufferUnderflowException newBOEReadUpper(@NonNegative long offset, long adding, boolean given) {
        long limit2 = given ? writeLimit() : readLimit();
        return new DecoratedBufferUnderflowException(String
                .format("readCheckOffset0 failed. Offset: %d + adding: %d > limit: %d (given: %s)", offset, adding, limit2, given));
    }

    @NotNull
    private DecoratedBufferUnderflowException newBOEReadLower(@NonNegative long offset) {
        return new DecoratedBufferUnderflowException(String.format("readCheckOffset0 failed. Offset: %d < start: %d", offset, start()));
    }

    void prewriteCheckOffset(@NonNegative long offset, long subtracting)
            throws BufferOverflowException, IllegalStateException {
        if (BYTES_BOUNDS_UNCHECKED)
            return;
        prewriteCheckOffset0(offset, subtracting);
    }

    private void prewriteCheckOffset0(@NonNegative long offset, long subtracting)
            throws BufferOverflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if ((offset - subtracting) < start()) {
            throw newBOERange(offset, subtracting, "prewriteCheckOffset0 failed. Offset: %d - subtracting: %d < start: %d", start());
        }
        long limit0 = readLimit();
        if (offset > limit0) {
            throw new DecoratedBufferOverflowException(
                    String.format("prewriteCheckOffset0 failed. Offset: %d > readLimit: %d", offset, limit0));
        }
    }

    @NotNull
    @Override
    public Bytes<U> writeByte(byte i8)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(1, 1);
        bytesStore.writeByte(offset, i8);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> prewrite(final byte[] bytes)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = prewriteOffsetPositionMoved(bytes.length);
        bytesStore.write(offset, bytes);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> prewrite(@NotNull BytesStore bytes)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = prewriteOffsetPositionMoved(bytes.readRemaining());
        bytesStore.write(offset, bytes);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> prewriteByte(byte i8)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = prewriteOffsetPositionMoved(1);
        bytesStore.writeByte(offset, i8);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> prewriteInt(int i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = prewriteOffsetPositionMoved(4);
        bytesStore.writeInt(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> prewriteShort(short i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = prewriteOffsetPositionMoved(2);
        bytesStore.writeShort(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> prewriteLong(long l)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = prewriteOffsetPositionMoved(8);
        bytesStore.writeLong(offset, l);
        return this;
    }

    protected final long writeOffsetPositionMoved(@NonNegative long adding)
            throws BufferOverflowException, IllegalStateException {
        return writeOffsetPositionMoved(adding, adding);
    }

    protected long writeOffsetPositionMoved(@NonNegative long adding, @NonNegative long advance)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long oldPosition = writePosition();
        writeCheckOffset(oldPosition, adding);
        uncheckedWritePosition(writePosition() + advance);
        return oldPosition;
    }

    protected void uncheckedWritePosition(@NonNegative long writePosition) {
        // No assert of thread safety here needed
        this.writePosition = writePosition;
    }

    protected long prewriteOffsetPositionMoved(@NonNegative long subtracting)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        prewriteCheckOffset(readPosition, subtracting);
        readPosition -= subtracting;
        return readPosition;
    }

    @NotNull
    @Override
    public Bytes<U> writeShort(short i16)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(2);
        bytesStore.writeShort(offset, i16);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeInt(int i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(4);
        bytesStore.writeInt(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeIntAdv(int i, @NonNegative int advance)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(4, advance);
        bytesStore.writeInt(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeLong(long i64)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(8);
        bytesStore.writeLong(offset, i64);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeLongAdv(long i64, @NonNegative int advance)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(8, advance);
        bytesStore.writeLong(offset, i64);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeFloat(float f)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(4);
        bytesStore.writeFloat(offset, f);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeDouble(double d)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(8);
        bytesStore.writeDouble(offset, d);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeDoubleAndInt(double d, int i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(12);
        bytesStore.writeDouble(offset, d);
        bytesStore.writeInt(offset + 8, i);
        return this;
    }

    @Override
    public int read(byte[] bytes, @NonNegative int off, @NonNegative int len) throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        requireNonNull(bytes);
        long remaining = readRemaining();
        if (remaining <= 0)
            return -1;
        final int totalToCopy = (int) Math.min(len, remaining);
        int remainingToCopy = totalToCopy;
        int currentOffset = off;
        while (remainingToCopy > 0) {
            int currentBatchSize = Math.min(remainingToCopy, safeCopySize());
            long offsetInRDO = readOffsetPositionMoved(currentBatchSize);
            bytesStore.read(offsetInRDO, bytes, currentOffset, currentBatchSize);
            currentOffset += currentBatchSize;
            remainingToCopy -= currentBatchSize;
        }
        return totalToCopy;
    }

    @Override
    public long read(@NonNegative long offsetInRDI, byte[] bytes, @NonNegative int offset, @NonNegative int length) throws IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore.read(offsetInRDI, bytes, offset, length);
    }

    @NotNull
    @Override
    public Bytes<U> write(final byte[] byteArray,
                          @NonNegative final int offset,
                          @NonNegative final int length) throws BufferOverflowException, IllegalStateException, IllegalArgumentException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        requireNonNegative(offset);
        requireNonNegative(length);
        if ((length + offset) > byteArray.length) {
            throw new DecoratedBufferOverflowException("bytes.length=" + byteArray.length + ", " + "length=" + length + ", offset=" + offset);
        }
        if (length > writeRemaining()) {
            throw new DecoratedBufferOverflowException(
                    String.format("write failed. Length: %d > writeRemaining: %d", length, writeRemaining()));
        }
        ensureCapacity(writePosition() + length);
        int remaining = length;
        int pos = offset;
        while (remaining > 0) {
            int copy = Math.min(remaining, safeCopySize()); // copy 64 KB at a time.
            long offsetInRDO = writeOffsetPositionMoved(copy);
            bytesStore.write(offsetInRDO, byteArray, pos, copy);
            pos += copy;
            remaining -= copy;
        }
        return this;
    }

    protected int safeCopySize() {
        return 64 << 10;
    }

    @NotNull
    @Override
    public Bytes<U> writeSome(@NotNull ByteBuffer buffer)
            throws BufferOverflowException, IllegalStateException, BufferUnderflowException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        int length = (int) Math.min(buffer.remaining(), writeRemaining());
        try {
            ensureCapacity(writePosition() + length);
        } catch (IllegalArgumentException e) {
            throw new AssertionError(e);
        }
        bytesStore.write(writePosition(), buffer, buffer.position(), length);
        uncheckedWritePosition(writePosition() + length);
        buffer.position(buffer.position() + length);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeOrderedInt(int i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(4);
        bytesStore.writeOrderedInt(offset, i);
        return this;
    }

    @NotNull
    @Override
    public Bytes<U> writeOrderedLong(long i)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        long offset = writeOffsetPositionMoved(8);
        bytesStore.writeOrderedLong(offset, i);
        return this;
    }

    @Override
    public long addressForRead(@NonNegative long offset)
            throws BufferUnderflowException, IllegalStateException {
        replaceByteStoreIfEmpty();
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore.addressForRead(offset);
    }

    @Override
    public long addressForWrite(@NonNegative long offset)
            throws BufferOverflowException, IllegalStateException {
        replaceByteStoreIfEmpty();
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore.addressForWrite(offset);
    }

    @Override
    public long addressForWritePosition()
            throws BufferOverflowException, IllegalStateException {
        replaceByteStoreIfEmpty();
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore.addressForWrite(writePosition());
    }

    @Override
    public int hashCode() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return HashCodeEqualsUtil.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return obj instanceof BytesStore && BytesInternal.contentEqual(this, (BytesStore) obj);
    }

    @NotNull
    @Override
    public String toString() {
        // We do not check thread safety for toString as this might blow up debuggers etc.
        // Reserving prevents illegal access to this Bytes object if released by another thread
        final ReferenceOwner toStringOwner = ReferenceOwner.temporary("toString");
        reserve(toStringOwner);
        try {
            return BytesInternal.toString(this);
        } catch (Exception e) {
            return e.toString();
        } finally {
            release(toStringOwner);
        }
    }

    @Override
    public void nativeRead(@NonNegative long position, long address, @NonNegative long size)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        bytesStore.nativeRead(position, address, size);
    }

    @Override
    public void nativeWrite(long address, @NonNegative long position, @NonNegative long size)
            throws BufferOverflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        bytesStore.nativeWrite(address, position, size);
    }

    @NotNull
    @Override
    public BytesStore bytesStore() {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        return bytesStore;
    }

    protected void bytesStore(@NotNull final BytesStore<Bytes<U>, U> bytesStore) {
        if (!isElastic()) {
            throw new IllegalStateException("Attempting to set byteStore on a Bytes instance that is not elastic: " + getClass().getName());
        }
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        this.bytesStore = requireNonNull(bytesStore);
    }

    @Override
    public int lastDecimalPlaces() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return lastDecimalPlaces;
    }

    @Override
    public void lastDecimalPlaces(int lastDecimalPlaces) {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        this.lastDecimalPlaces = Math.max(0, lastDecimalPlaces);
    }

    @Override
    public boolean lastNumberHadDigits() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return lastNumberHadDigits;
    }

    @Override
    public void lastNumberHadDigits(boolean lastNumberHadDigits) {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        this.lastNumberHadDigits = lastNumberHadDigits;
    }

    @Override
    public BytesStore<Bytes<U>, U> copy() throws IllegalStateException {
        return null;
    }

    @Override
    public boolean isElastic() {
        return false;
    }

    @Override
    public void lenient(boolean lenient) {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        this.lenient = lenient;
    }

    @Override
    public boolean lenient() {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return lenient;
    }

    @Override
    public int byteCheckSum()
            throws IORuntimeException, BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return byteCheckSum(readPosition(), readLimit());
    }

    @Override
    public int byteCheckSum(@NonNegative long start, @NonNegative long end)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        if (end < Integer.MAX_VALUE && isDirectMemory())
            return byteCheckSum((int) start, (int) end);
        return Bytes.super.byteCheckSum(start, end);
    }

    @Override
    public boolean startsWith(@Nullable final BytesStore bytesStore) throws IllegalStateException {
        // This class implements HasUncheckedRandomDataInput, so we could potentially use
        // the unchecked version of startsWith
        assert AssertUtil.SKIP_ASSERTIONS || threadSafetyCheck(true);
        return bytesStore != null && BytesInternal.startsWithUnchecked(this, bytesStore);
    }

    @Override
    public @NotNull UncheckedRandomDataInput acquireUncheckedInput() {
        return uncheckedRandomDataInput;
    }

    private final class UncheckedRandomDataInputHolder implements UncheckedRandomDataInput {

        @Override
        public byte readByte(@NonNegative long offset) {
            assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
            return bytesStore.readByte(offset);
        }

        @Override
        public short readShort(@NonNegative long offset) {
            assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
            return bytesStore.readShort(offset);
        }

        @Override
        public int readInt(@NonNegative long offset) {
            assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
            return bytesStore.readInt(offset);
        }

        @Override
        public long readLong(@NonNegative long offset) {
            assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
            return bytesStore.readLong(offset);
        }
    }

    public int byteCheckSum(@NonNegative int start, @NonNegative int end)
            throws BufferUnderflowException, IllegalStateException {
        assert AssertUtil.SKIP_ASSERTIONS || !isElastic() || threadSafetyCheck(true);
        int sum = 0;
        for (int i = start; i < end; i++) {
            sum += readByte(i);
        }
        return sum & 0xFF;
    }

    @Override
    public boolean isImmutableEmptyByteStore() {
        return bytesStore.isImmutableEmptyByteStore();
    }

    private void replaceByteStoreIfEmpty() {
        if (isImmutableEmptyByteStore()) {
            // This forces a replacement of the underlying ByteStore
            write(EMPTY_ARRAY);
        }
    }

    static final class ReportUnoptimised {
        static {
            Jvm.reportUnoptimised();
        }

        private ReportUnoptimised() {
        }

        static void reportOnce() {
            // Do nothing
        }
    }
}
