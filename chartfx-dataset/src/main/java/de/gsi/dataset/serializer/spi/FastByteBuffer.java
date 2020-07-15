package de.gsi.dataset.serializer.spi;

import static sun.misc.Unsafe.ARRAY_BOOLEAN_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_FLOAT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_SHORT_BASE_OFFSET;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.gsi.dataset.serializer.IoBuffer;
import de.gsi.dataset.utils.AssertUtils;

import sun.misc.Unsafe;

// import static jdk.internal.misc.Unsafe; // NOPMD by rstein TODO replaces sun in JDK11

/**
 * FastByteBuffer implementation based on JVM 'Unsafe' Class. based on:
 * https://mechanical-sympathy.blogspot.com/2012/07/native-cc-like-performance-for-java.html
 * http://java-performance.info/various-methods-of-binary-serialization-in-java/
 *
 * @author rstein
 */
@SuppressWarnings("restriction")
public class FastByteBuffer implements IoBuffer {
    public static final int SIZE_OF_BOOLEAN = 1;
    public static final int SIZE_OF_BYTE = 1;
    public static final int SIZE_OF_SHORT = 2;
    public static final int SIZE_OF_CHAR = 2;
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_LONG = 8;
    public static final int SIZE_OF_FLOAT = 4;
    public static final int SIZE_OF_DOUBLE = 8;
    private static final int DEFAULT_INITIAL_CAPACITY = 1 << 10;
    private static final int DEFAULT_MIN_CAPACITY_INCREASE = 1 << 10;
    private static final int DEFAULT_MAX_CAPACITY_INCREASE = 100 * (1 << 10);
    private static final Unsafe unsafe; // NOPMD
    static {
        // get an instance of the otherwise private 'Unsafe' class
        try {
            Class<?> cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
            Field logger = cls.getDeclaredField("logger");

            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            unsafe.putObjectVolatile(cls, unsafe.staticFieldOffset(logger), null);

        } catch (NoSuchFieldException | SecurityException | IllegalAccessException | ClassNotFoundException e) {
            throw new SecurityException(e); // NOPMD
        }
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private int position;
    private int limit;
    private byte[] buffer;
    private boolean enforceSimpleStringEncoding = false;
    private Runnable callBackFunction = () -> {};

    /**
     * construct new FastByteBuffer
     */
    public FastByteBuffer() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * construct new FastByteBuffer
     *
     * @param buffer buffer to initialise/re-use
     * @param limit position until buffer is filled
     */
    public FastByteBuffer(final byte[] buffer, final int limit) {
        AssertUtils.notNull("buffer", buffer);
        if (buffer.length < limit) {
            throw new IllegalArgumentException(String.format("limit %d >= capacity %d", limit, buffer.length));
        }
        this.buffer = buffer;
        this.limit = limit;
        position = 0;
    }

    /**
     * construct new FastByteBuffer
     *
     * @param size initial capacity of the buffer
     */
    public FastByteBuffer(final int size) {
        AssertUtils.gtEqThanZero("size", size);
        buffer = new byte[size];
        position = 0;
        limit = buffer.length;
    }

    @Override
    public int capacity() {
        return buffer.length;
    }

    @Override
    public void clear() {
        position = 0;
        limit = capacity();
    }

    public byte[] elements() {
        return buffer;
    }

    @Override
    public void ensureAdditionalCapacity(final int capacity) {
        final int neededTotalCapacity = this.position() + capacity;
        if (neededTotalCapacity < capacity()) {
            return;
        }
        if (position > capacity()) {
            throw new IllegalStateException("position " + position + " is beyond buffer capacity " + capacity());
        }
        //TODO: add smarter enlarging algorithm (ie. increase fast for small arrays, + n% for medium sized arrays, byte-by-byte for large arrays)
        final int addCapacity = Math.min(Math.max(DEFAULT_MIN_CAPACITY_INCREASE, neededTotalCapacity >> 3), DEFAULT_MAX_CAPACITY_INCREASE);
        forceCapacity(neededTotalCapacity + addCapacity, capacity());
    }

    @Override
    public void ensureCapacity(final int newCapacity) {
        if (newCapacity <= capacity()) {
            return;
        }
        forceCapacity(newCapacity, capacity());
    }

    /**
     * Forces FastByteBUffer to contain the given number of entries, preserving just a part of the array.
     *
     * @param length the new minimum length for this array.
     * @param preserve the number of elements of the old buffer that shall be preserved in case a new allocation is
     *        necessary.
     */
    @Override
    public void forceCapacity(final int length, final int preserve) {
        if (length == capacity()) {
            return;
        }
        final byte[] newBuffer = new byte[length];
        final int bytesToCopy = preserve * SIZE_OF_BYTE;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET, newBuffer, ARRAY_BYTE_BASE_OFFSET, bytesToCopy);
        position = (position < newBuffer.length) ? position : newBuffer.length - 1;
        buffer = newBuffer;
        limit = buffer.length;
    }

    @Override
    public int[] getArraySizeDescriptor() {
        final int nDims = getInt(); // number of dimensions
        final int[] ret = new int[nDims];
        for (int i = 0; i < nDims; i++) {
            ret[i] = getInt(); // vector size for each dimension
        }
        return ret;
    }

    @Override
    public boolean getBoolean() { // NOPMD by rstein
        final boolean value = unsafe.getBoolean(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
        position += SIZE_OF_BOOLEAN;

        return value;
    }

    @Override
    public boolean[] getBooleanArray(final boolean[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final boolean[] values = initNeeded ? new boolean[arraySize + offset] : dst;

        final int bytesToCopy = initNeeded ? arraySize : Math.min(arraySize, length);
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values, ARRAY_BOOLEAN_BASE_OFFSET + offset, bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public byte getByte() {
        final byte value = unsafe.getByte(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
        position += SIZE_OF_BYTE;

        return value;
    }

    @Override
    public byte[] getByteArray(final byte[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final byte[] values = initNeeded ? new byte[arraySize + offset] : dst;

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length));
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values, ARRAY_BYTE_BASE_OFFSET + offset, bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public Runnable getCallBackFunction() {
        return callBackFunction;
    }

    @Override
    public char getChar() {
        final char value = unsafe.getChar(buffer, (long) ARRAY_CHAR_BASE_OFFSET + position);
        position += SIZE_OF_CHAR;

        return value;
    }

    @Override
    public char[] getCharArray(final char[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final char[] values = initNeeded ? new char[arraySize + offset] : dst;

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length)) * SIZE_OF_CHAR;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values, ARRAY_SHORT_BASE_OFFSET + (offset * SIZE_OF_CHAR),
                bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public double getDouble() {
        final double value = unsafe.getDouble(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
        position += SIZE_OF_DOUBLE;

        return value;
    }

    @Override
    public double[] getDoubleArray(final double[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final double[] values = initNeeded ? new double[arraySize + offset] : dst;

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length)) * SIZE_OF_DOUBLE;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values,
                ARRAY_DOUBLE_BASE_OFFSET + (offset * SIZE_OF_DOUBLE), bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public float getFloat() {
        final float value = unsafe.getFloat(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
        position += SIZE_OF_FLOAT;

        return value;
    }

    @Override
    public float[] getFloatArray(final float[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final float[] values = initNeeded ? new float[arraySize + offset] : dst;

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length)) * SIZE_OF_FLOAT;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values,
                ARRAY_FLOAT_BASE_OFFSET + (offset * SIZE_OF_FLOAT), bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public int getInt() {
        final int value = unsafe.getInt(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
        position += SIZE_OF_INT;

        return value;
    }

    @Override
    public int[] getIntArray(final int[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final int[] values = initNeeded ? new int[arraySize + offset] : dst;

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length)) * SIZE_OF_INT;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values, ARRAY_INT_BASE_OFFSET + (offset * SIZE_OF_INT),
                bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public long getLong() {
        final long value = unsafe.getLong(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position);
        position += SIZE_OF_LONG;

        return value;
    }

    @Override
    public long[] getLongArray(final long[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final long[] values = initNeeded ? new long[arraySize + offset] : dst;

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length)) * SIZE_OF_LONG;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values, ARRAY_LONG_BASE_OFFSET + (offset * SIZE_OF_LONG),
                bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public short getShort() { // NOPMD by rstein
        final short value = unsafe.getShort(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position); // NOPMD
        position += SIZE_OF_SHORT;

        return value;
    }

    @Override
    public short[] getShortArray(final short[] dst, final int offset, final int length) { // NOPMD by rstein
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final short[] values = initNeeded ? new short[arraySize + offset] : dst; // NOPMD by rstein

        final int bytesToCopy = (initNeeded ? arraySize : Math.min(arraySize, length)) * SIZE_OF_SHORT;
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET + position, values,
                ARRAY_SHORT_BASE_OFFSET + (offset * SIZE_OF_SHORT), bytesToCopy);
        position += bytesToCopy;

        return values;
    }

    @Override
    public String getString() {
        if (isEnforceSimpleStringEncoding()) {
            return this.getStringISO8859();
        }
        final int arraySize = getInt(); // for C++ zero terminated string
        final String str = new String(buffer, position, arraySize - 1, StandardCharsets.UTF_8);
        position += arraySize; // N.B. +1 larger to be compatible with C++ zero terminated string
        return str;
    }

    @Override
    public String[] getStringArray(final String[] dst, final int offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean initNeeded = dst == null || length < 0;
        final String[] ret = initNeeded ? new String[arraySize + offset] : dst;
        final int size = initNeeded ? arraySize : Math.min(arraySize, length);
        for (int k = 0; k < size; k++) {
            ret[k + offset] = getString();
        }
        return ret;
    }

    @Override
    public String getStringISO8859() {
        final int arraySize = getInt(); // for C++ zero terminated string
        //alt safe-fallback final String str = new String(buffer,  position, arraySize - 1, StandardCharsets.ISO_8859_1);
        final String str = new String(buffer, 0, position, arraySize - 1); // NOPMD NOSONAR fastest alternative
        // final String str = FastStringBuilder.iso8859BytesToString(buffer, position, arraySize - 1);
        position += arraySize; // N.B. +1 larger to be compatible with C++ zero terminated string
        return str;
    }

    @Override
    public boolean hasRemaining() {
        return (this.position() < capacity());
    }

    @Override
    public boolean isEnforceSimpleStringEncoding() {
        return enforceSimpleStringEncoding;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public void limit(final int newLimit) {
        if ((newLimit > capacity()) || (newLimit < 0)) {
            throw new IllegalArgumentException(String.format("invalid newLimit: [0, position: %d, newLimit:%d, %d]",
                    position, newLimit, capacity()));
        }
        limit = newLimit;
        if (position > limit) {
            position = limit;
        }
    }

    @Override
    public ReadWriteLock lock() {
        return lock;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(final int newPosition) {
        if ((newPosition > limit) || (newPosition < 0) || (newPosition >= capacity())) {
            throw new IllegalArgumentException(String.format("invalid newPosition: %d vs. [0, position=%d, limit:%d, capacity:%d]", newPosition, position, limit, capacity()));
        }
        position = newPosition;
    }

    @Override
    public int putArraySizeDescriptor(final int n) {
        putInt(1); // number of dimensions
        putInt(n); // vector size for each dimension
        putInt(n); // strided-array size
        return n;
    }

    @Override
    public int putArraySizeDescriptor(final int[] dims) {
        putInt(dims.length); // number of dimensions
        int nElements = 1;
        for (final int dim : dims) {
            nElements *= dim;
            putInt(dim); // vector size for each dimension
        }
        putInt(nElements); // strided-array size
        return nElements;
    }

    @Override
    public void putBoolean(final boolean value) {
        unsafe.putBoolean(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_BOOLEAN;
    }

    @Override
    public void putBooleanArray(final boolean[] values, final int offset, final int n) {
        final int bytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length);
        ensureAdditionalCapacity(bytesToCopy);
        copyMemory(values, ARRAY_BOOLEAN_BASE_OFFSET + offset, buffer, ARRAY_BYTE_BASE_OFFSET + position, bytesToCopy);
        position += bytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putBooleanArray(final boolean[] values, final int offset, final int[] dims) {
        final int bytesToCopy = putArraySizeDescriptor(dims);
        ensureAdditionalCapacity(bytesToCopy);
        copyMemory(values, ARRAY_BOOLEAN_BASE_OFFSET + offset, buffer, ARRAY_BYTE_BASE_OFFSET + position, bytesToCopy);
        position += bytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putByte(final byte value) {
        unsafe.putByte(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_BYTE;
    }

    @Override
    public void putByteArray(final byte[] values, final int offset, final int n) {
        final int bytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length);
        ensureAdditionalCapacity(bytesToCopy);
        copyMemory(values, ARRAY_BOOLEAN_BASE_OFFSET + offset, buffer, ARRAY_BYTE_BASE_OFFSET + position, bytesToCopy);
        position += bytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putByteArray(final byte[] values, final int offset, final int[] dims) {
        final int bytesToCopy = putArraySizeDescriptor(dims);
        ensureAdditionalCapacity(bytesToCopy);
        copyMemory(values, ARRAY_BOOLEAN_BASE_OFFSET + offset, buffer, ARRAY_BYTE_BASE_OFFSET + position, bytesToCopy);
        position += bytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putChar(final char value) {
        unsafe.putChar(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_CHAR;
    }

    @Override
    public void putCharArray(final char[] values, final int offset, final int n) {
        final int arrayOffset = ARRAY_CHAR_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_CHAR;
        final int nBytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putCharArray(final char[] values, final int offset, final int[] dims) {
        final int arrayOffset = ARRAY_CHAR_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_CHAR;
        final int nBytesToCopy = putArraySizeDescriptor(dims) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putDouble(final double value) {
        unsafe.putDouble(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_DOUBLE;
    }

    @Override
    public void putDoubleArray(final double[] values, final int offset, final int n) {
        final int arrayOffset = ARRAY_DOUBLE_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_DOUBLE;
        final int nBytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putDoubleArray(final double[] values, final int offset, final int[] dims) {
        final int arrayOffset = ARRAY_DOUBLE_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_DOUBLE;
        final int nBytesToCopy = putArraySizeDescriptor(dims) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putEndMarker(final String markerName) {
        // empty implementation
    }

    @Override
    public void putFloat(final float value) {
        unsafe.putFloat(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_FLOAT;
    }

    @Override
    public void putFloatArray(final float[] values, final int offset, final int n) {
        final int arrayOffset = ARRAY_FLOAT_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_FLOAT;
        final int nBytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putFloatArray(final float[] values, final int offset, final int[] dims) {
        final int arrayOffset = ARRAY_FLOAT_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_FLOAT;
        final int nBytesToCopy = putArraySizeDescriptor(dims) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putInt(final int value) {
        unsafe.putInt(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_INT;
    }

    @Override
    public void putIntArray(final int[] values, final int offset, final int n) {
        final int arrayOffset = ARRAY_INT_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_INT;
        final int nBytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putIntArray(final int[] values, final int offset, final int[] dims) {
        final int arrayOffset = ARRAY_INT_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_INT;
        final int nBytesToCopy = putArraySizeDescriptor(dims) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putLong(final long value) {
        unsafe.putLong(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_LONG;
    }

    @Override
    public void putLongArray(final long[] values, final int offset, final int n) {
        final int arrayOffset = ARRAY_LONG_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_LONG;
        final int nBytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putLongArray(final long[] values, final int offset, final int[] dims) {
        final int arrayOffset = ARRAY_LONG_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_LONG;
        final int nBytesToCopy = putArraySizeDescriptor(dims) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putShort(final short value) { // NOPMD by rstein
        unsafe.putShort(buffer, (long) ARRAY_BYTE_BASE_OFFSET + position, value);
        position += SIZE_OF_SHORT;
    }

    @Override
    public void putShortArray(final short[] values, final int offset, final int n) { // NOPMD by rstein
        final int arrayOffset = ARRAY_SHORT_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_SHORT;
        final int nBytesToCopy = putArraySizeDescriptor(n > 0 ? Math.min(n, values.length) : values.length) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putShortArray(final short[] values, final int offset, final int[] dims) { // NOPMD by rstein
        final int arrayOffset = ARRAY_SHORT_BASE_OFFSET;
        final int primitiveSize = SIZE_OF_SHORT;
        final int nBytesToCopy = putArraySizeDescriptor(dims) * primitiveSize;
        ensureAdditionalCapacity(nBytesToCopy);
        copyMemory(values, arrayOffset + offset * primitiveSize, buffer, arrayOffset + position, nBytesToCopy);
        position += nBytesToCopy;
        callBackFunction.run();
    }

    @Override
    public void putStartMarker(final String markerName) {
        // empty implementation
    }

    @Override
    public void putString(final String string) {
        if (string == null) {
            putString("");
            callBackFunction.run();
            return;
        }
        if (isEnforceSimpleStringEncoding()) {
            putStringISO8859(string);
            callBackFunction.run();
            return;
        }
        final int utf16StringLength = string.length();
        final int initialPos = position;
        position += SIZE_OF_INT;
        // write string-to-byte (in-place)
        ensureAdditionalCapacity(3 * utf16StringLength + 1);
        final int strLength = encodeUTF8(string, buffer, ARRAY_BYTE_BASE_OFFSET, position, 3 * utf16StringLength);
        final int endPos = position + strLength;

        // write length of string byte representation
        position = initialPos;
        putInt(strLength + 1);
        position = endPos;

        putByte((byte) 0); // For C++ zero terminated string
        callBackFunction.run();
    }

    @Override
    public void putStringArray(final String[] values, final int offset, final int n) {
        final int nElements = n > 0 ? Math.min(n, values.length) : values.length;
        ensureAdditionalCapacity(putArraySizeDescriptor(nElements));
        final Runnable oldRunnable = callBackFunction;
        setCallBackFunction(null);
        if (isEnforceSimpleStringEncoding()) {
            for (int k = 0; k < nElements; k++) {
                putStringISO8859(values[k + offset]);
            }
            setCallBackFunction(oldRunnable);
            callBackFunction.run();
            return;
        }
        for (int k = 0; k < nElements; k++) {
            putString(values[k + offset]);
        }
        setCallBackFunction(oldRunnable);
        callBackFunction.run();
    }

    @Override
    public void putStringArray(final String[] values, final int offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        ensureAdditionalCapacity(nElements);
        final Runnable oldRunnable = callBackFunction;
        setCallBackFunction(null);
        if (isEnforceSimpleStringEncoding()) {
            for (int k = 0; k < nElements; k++) {
                putStringISO8859(values[k + offset]);
            }
            setCallBackFunction(oldRunnable);
            callBackFunction.run();
            return;
        }
        for (int k = 0; k < nElements; k++) {
            putString(values[k + offset]);
        }
        setCallBackFunction(oldRunnable);
        callBackFunction.run();
    }

    @Override
    public void putStringISO8859(final String string) {
        if (string == null) {
            putStringISO8859("");
            callBackFunction.run();
            return;
        }
        final int initialPos = position;
        position += SIZE_OF_INT;
        // write string-to-byte (in-place)
        final int strLength = encodeISO8859(string, buffer, ARRAY_BYTE_BASE_OFFSET, position, string.length());
        final int endPos = position + strLength;

        // write length of string byte representation
        position = initialPos;
        putInt(strLength + 1);
        position = endPos;

        putByte((byte) 0); // For C++ zero terminated string
        callBackFunction.run();
    }

    @Override
    public int remaining() {
        return limit - position;
    }

    @Override
    public void reset() {
        position = 0;
        limit = buffer.length;
    }

    @Override
    public void setCallBackFunction(final Runnable runnable) {
        if (runnable == null) {
            callBackFunction = () -> {};
            return;
        }
        callBackFunction = runnable;
    }

    @Override
    public void setEnforceSimpleStringEncoding(final boolean state) {
        this.enforceSimpleStringEncoding = state;
    }

    @Override
    public String toString() {
        return super.toString() + String.format(" - [0, position=%d, limit:%d, capacity:%d]", position, limit, capacity());
    }

    /**
     * Trims the internal buffer array so that the capacity is equal to the size.
     *
     * @see java.util.ArrayList#trimToSize()
     */
    @Override
    public void trim() {
        trim(position());
    }

    /**
     * Trims the internal buffer array if it is too large. If the current array length is smaller than or equal to
     * {@code n}, this method does nothing. Otherwise, it trims the array length to the maximum between
     * {@code requestedCapacity} and {@link #capacity()}.
     * <p>
     * This method is useful when reusing FastBuffers. {@linkplain #reset() Clearing a list} leaves the array length
     * untouched. If you are reusing a list many times, you can call this method with a typical size to avoid keeping
     * around a very large array just because of a few large transient lists.
     *
     * @param requestedCapacity the threshold for the trimming.
     */
    @Override
    public void trim(final int requestedCapacity) {
        if ((requestedCapacity >= capacity()) || (this.position() > requestedCapacity)) {
            return;
        }
        final int bytesToCopy = Math.min(Math.max(requestedCapacity, position()), capacity()) * SIZE_OF_BYTE;
        final byte[] newBuffer = new byte[bytesToCopy];
        copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET, newBuffer, ARRAY_BYTE_BASE_OFFSET, bytesToCopy);
        buffer = newBuffer;
        limit = newBuffer.length;
    }

    private static int encodeISO8859(final String sequence, final byte[] bytes, final long baseOffset, final int offset, final int length) {
        // encode to ISO_8859_1
        final long j = baseOffset + offset;
        for (int i = 0; i < length; i++) {
            unsafe.putByte(bytes, j + i, (byte) (sequence.charAt(i) & 0xFF));
        }
        return length;
    }

    private static int encodeUTF8(final CharSequence sequence, final byte[] bytes, final int baseOffset, final int offset, final int length) {
        int utf16Length = sequence.length();
        int j = baseOffset + offset;
        int i = 0;
        int limit = baseOffset + offset + length;
        // Designed to take advantage of https://wiki.openjdk.java.net/display/HotSpot/RangeCheckElimination
        for (char c; i < utf16Length && i + j < limit && (c = sequence.charAt(i)) < 0x80; i++) {
            unsafe.putByte(bytes, (long) j + i, (byte) c);
        }
        if (i == utf16Length) {
            return utf16Length;
        }
        j += i;
        for (; i < utf16Length; i++) {
            final char c = sequence.charAt(i);
            if (c < 0x80 && j < limit) {
                unsafe.putByte(bytes, j++, (byte) c);
            } else if (c < 0x800 && j <= limit - 2) { // 11 bits, two UTF-8 bytes
                unsafe.putByte(bytes, j++, (byte) ((0xF << 6) | (c >>> 6)));
                unsafe.putByte(bytes, j++, (byte) (0x80 | (0x3F & c)));
            } else if ((c < Character.MIN_SURROGATE || Character.MAX_SURROGATE < c) && j <= limit - 3) {
                // Maximum single-char code point is 0xFFFF, 16 bits, three UTF-8 bytes
                unsafe.putByte(bytes, j++, (byte) ((0xF << 5) | (c >>> 12)));
                unsafe.putByte(bytes, j++, (byte) (0x80 | (0x3F & (c >>> 6))));
                unsafe.putByte(bytes, j++, (byte) (0x80 | (0x3F & c)));
            } else if (j <= limit - 4) {
                // Minimum code point represented by a surrogate pair is 0x10000, 17 bits, four UTF-8 bytes
                final char low;
                if (i + 1 == sequence.length() || !Character.isSurrogatePair(c, (low = sequence.charAt(++i)))) {
                    throw new IllegalArgumentException("Unpaired surrogate at index " + (i - 1));
                }
                int codePoint = Character.toCodePoint(c, low);
                unsafe.putByte(bytes, j++, (byte) ((0xF << 4) | (codePoint >>> 18)));
                unsafe.putByte(bytes, j++, (byte) (0x80 | (0x3F & (codePoint >>> 12))));
                unsafe.putByte(bytes, j++, (byte) (0x80 | (0x3F & (codePoint >>> 6))));
                unsafe.putByte(bytes, j++, (byte) (0x80 | (0x3F & codePoint)));
            } else {
                throw new ArrayIndexOutOfBoundsException("Failed writing " + c + " at index " + j);
            }
        }
        return j - baseOffset - offset;
    }

    private static void copyMemory(final Object srcBase, final int srcOffset, final Object destBase, final int destOffset, final int nBytes) {
        unsafe.copyMemory(srcBase, srcOffset, destBase, destOffset, nBytes);
    }

    /**
     * Wraps a given byte array into FastByteBuffer
     * <p>
     * Note it is guaranteed that the type of the array returned by {@link #elements()} will be the same.
     *
     * @param byteArray an array to wrap.
     * @return a new FastByteBuffer of the given size, wrapping the given array.
     */
    public static FastByteBuffer wrap(final byte[] byteArray) {
        return wrap(byteArray, byteArray.length);
    }

    /**
     * Wraps a given byte array into FastByteBuffer
     * <p>
     * Note it is guaranteed that the type of the array returned by {@link #elements()} will be the same.
     *
     * @param byteArray an array to wrap.
     * @param length the length of the resulting array list.
     * @return a new FastByteBuffer of the given size, wrapping the given array.
     */
    public static FastByteBuffer wrap(final byte[] byteArray, final int length) {
        return new FastByteBuffer(byteArray, length);
    }
}
