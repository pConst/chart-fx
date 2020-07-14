package de.gsi.dataset.serializer.spi;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.gsi.dataset.serializer.IoBuffer;

/**
 * @author rstein
 */
public class ByteBuffer implements IoBuffer {
    public static final long SIZE_OF_BOOLEAN = 1;
    public static final long SIZE_OF_BYTE = 1;
    public static final long SIZE_OF_SHORT = 2;
    public static final long SIZE_OF_CHAR = 2;
    public static final long SIZE_OF_INT = 4;
    public static final long SIZE_OF_LONG = 8;
    public static final long SIZE_OF_FLOAT = 4;
    public static final long SIZE_OF_DOUBLE = 8;
    private static final int DEFAULT_INITIAL_CAPACITY = 1000;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final java.nio.ByteBuffer nioByteBuffer;
    private boolean enforceSimpleStringEncoding = false;
    private Runnable callBackFunction;

    /**
     * construct new java.nio.ByteBuffer-based ByteBuffer with DEFAULT_INITIAL_CAPACITY
     */
    public ByteBuffer() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * construct new java.nio.ByteBuffer-based ByteBuffer with DEFAULT_INITIAL_CAPACITY
     *
     * @param nCapacity initial capacity
     */
    public ByteBuffer(final int nCapacity) {
        nioByteBuffer = java.nio.ByteBuffer.wrap(new byte[nCapacity]);
        nioByteBuffer.mark();
    }

    @Override
    public int capacity() {
        return nioByteBuffer.capacity();
    }

    @Override
    public void clear() {
        nioByteBuffer.clear();
    }

    @Override
    public void ensureAdditionalCapacity(final long capacity) {
        /* not implemented */
    }

    @Override
    public void ensureCapacity(final long capacity) {
        /* not implemented */
    }

    @Override
    public void forceCapacity(final long length, final long preserve) {
        /* not implemented */
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
    public boolean getBoolean() {
        return nioByteBuffer.get() > 0;
    }

    @Override
    public boolean[] getBooleanArray(final boolean[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final boolean[] ret = dst == null ? new boolean[arraySize + (int) offset] : dst;

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getBoolean();
        }
        return ret;
    }

    @Override
    public byte getByte() {
        return nioByteBuffer.get();
    }

    @Override
    public byte[] getByteArray(final byte[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final byte[] ret = dst == null ? new byte[bytesToCopy + (int) offset] : dst;
        nioByteBuffer.get(ret, (int) offset, bytesToCopy);
        return ret;
    }

    @Override
    public Runnable getCallBackFunction() {
        return callBackFunction;
    }

    @Override
    public char getChar() {
        return nioByteBuffer.getChar();
    }

    @Override
    public char[] getCharArray(final char[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final char[] ret = dst == null ? new char[arraySize + (int) offset] : dst;

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getChar();
        }
        return ret;
    }

    @Override
    public double getDouble() {
        return nioByteBuffer.getDouble();
    }

    @Override
    public double[] getDoubleArray(final double[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final double[] ret = dst == null ? new double[arraySize + (int) offset] : dst;

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getDouble();
        }
        return ret;
    }

    @Override
    public float getFloat() {
        return nioByteBuffer.getFloat();
    }

    @Override
    public float[] getFloatArray(final float[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final float[] ret = dst == null ? new float[arraySize + (int) offset] : dst;

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getFloat();
        }
        return ret;
    }

    @Override
    public int getInt() {
        return nioByteBuffer.getInt();
    }

    @Override
    public int[] getIntArray(final int[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final int[] ret = dst == null ? new int[arraySize + (int) offset] : dst;

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getInt();
        }
        return ret;
    }

    @Override
    public long getLong() {
        return nioByteBuffer.getLong();
    }

    @Override
    public long[] getLongArray(final long[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final long[] ret = dst == null ? new long[arraySize + (int) offset] : dst;

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getLong();
        }
        return ret;
    }

    @Override
    public short getShort() { // NOPMD
        return nioByteBuffer.getShort();
    }

    @Override
    public short[] getShortArray(final short[] dst, final long offset, final int length) { // NOPMD
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final short[] ret = dst == null ? new short[arraySize + (int) offset] : dst; // NOPMD

        final int bytesToCopy = (dst == null ? arraySize : Math.min(arraySize, length));
        final int end = (int) offset + bytesToCopy;
        for (int i = (int) offset; i < end; i++) {
            ret[i] = getShort();
        }
        return ret;
    }

    @Override
    public String getString() {
        final int arraySize = getInt() - 1; // for C++ zero terminated string
        final byte[] values = new byte[arraySize];
        nioByteBuffer.get(values, 0, arraySize);
        getByte(); // For C++ zero terminated string
        return new String(values, 0, arraySize, StandardCharsets.UTF_8);
    }

    @Override
    public String[] getStringArray(final String[] dst, final long offset, final int length) {
        getArraySizeDescriptor();
        final int arraySize = getInt(); // strided-array size
        final String[] ret = dst == null ? new String[arraySize] : dst;
        final int size = dst == null ? arraySize : Math.min(arraySize, length);
        for (int k = 0; k < size; k++) {
            ret[k + (int) offset] = getString();
        }
        return ret;
    }

    @Override
    public String getStringISO8859() {
        final int arraySize = getInt() - 1; // for C++ zero terminated string
        final byte[] values = new byte[arraySize];
        nioByteBuffer.get(values, 0, arraySize);
        getByte(); // For C++ zero terminated string
        return new String(values, 0, arraySize, StandardCharsets.ISO_8859_1);
    }

    @Override
    public boolean hasRemaining() {
        return nioByteBuffer.hasRemaining();
    }

    @Override
    public boolean isEnforceSimpleStringEncoding() {
        return enforceSimpleStringEncoding;
    }

    @Override
    public boolean isReadOnly() {
        return nioByteBuffer.isReadOnly();
    }

    @Override
    public long limit() {
        return nioByteBuffer.limit();
    }

    @Override
    public void limit(final int newLimit) {
        nioByteBuffer.limit(newLimit);
    }

    @Override
    public ReadWriteLock lock() {
        return lock;
    }

    @Override
    public long position() {
        return nioByteBuffer.position();
    }

    @Override
    public void position(final long newPosition) {
        nioByteBuffer.position((int) newPosition);
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
        putByte((byte) (value ? 1 : 0));
    }

    @Override
    public void putBooleanArray(final boolean[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements);
        for (int i = (int) offset; i < end; i++) {
            putBoolean(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putBooleanArray(final boolean[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements);
        for (int i = (int) offset; i < end; i++) {
            putBoolean(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putByte(final byte b) {
        nioByteBuffer.put(b);
    }

    @Override
    public void putByteArray(final byte[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        ensureAdditionalCapacity(nElements);
        nioByteBuffer.put(src, (int) offset, nElements);
        callBackFunction();
    }

    @Override
    public void putByteArray(final byte[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        ensureAdditionalCapacity(nElements);
        nioByteBuffer.put(src, (int) offset, nElements);
        callBackFunction();
    }

    @Override
    public void putChar(final char value) {
        nioByteBuffer.putChar(value);
    }

    @Override
    public void putCharArray(final char[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_CHAR);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putChar(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putCharArray(final char[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_CHAR);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putChar(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putDouble(final double value) {
        nioByteBuffer.putDouble(value);
    }

    @Override
    public void putDoubleArray(final double[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_DOUBLE);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putDouble(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putDoubleArray(final double[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_DOUBLE);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putDouble(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putEndMarker(final String markerName) {
        // empty implementation
    }

    @Override
    public void putFloat(final float value) {
        nioByteBuffer.putFloat(value);
    }

    @Override
    public void putFloatArray(final float[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_FLOAT);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putFloat(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putFloatArray(final float[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_FLOAT);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putFloat(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putInt(final int value) {
        nioByteBuffer.putInt(value);
    }

    @Override
    public void putIntArray(final int[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_INT);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putInt(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putIntArray(final int[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_INT);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putInt(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putLong(final long value) {
        nioByteBuffer.putLong(value);
    }

    @Override
    public void putLongArray(final long[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_LONG);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putLong(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putLongArray(final long[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_LONG);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putLong(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putShort(final short value) { // NOPMD
        nioByteBuffer.putShort(value);
    }

    @Override
    public void putShortArray(final short[] src, final long offset, final int n) { // NOPMD
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_SHORT);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putShort(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putShortArray(final short[] src, final long offset, final int[] dims) { // NOPMD
        final int nElements = putArraySizeDescriptor(dims);
        final int end = (int) offset + nElements;
        ensureAdditionalCapacity(nElements * SIZE_OF_SHORT);
        for (int i = (int) offset; i < end; i++) {
            nioByteBuffer.putShort(src[i + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putStartMarker(final String markerName) {
        // empty implementation
    }

    @Override
    public void putString(final String string) {
        if (isEnforceSimpleStringEncoding()) {
            this.putStringISO8859(string);
            return;
        }

        if (string == null) {
            putInt(1); // for C++ zero terminated string$
            putByte((byte) 0); // For C++ zero terminated string
            callBackFunction();
            return;
        }

        final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        putInt(bytes.length + 1); // for C++ zero terminated string$
        ensureAdditionalCapacity(bytes.length + 1L);
        nioByteBuffer.put(bytes, 0, bytes.length);
        putByte((byte) 0); // For C++ zero terminated string
        callBackFunction();
    }

    @Override
    public void putStringArray(final String[] src, final long offset, final int n) {
        final int nElements = putArraySizeDescriptor(n > 0 ? Math.min(n, src.length) : src.length);
        if (isEnforceSimpleStringEncoding()) {
            for (int k = 0; k < nElements; k++) {
                putStringISO8859(src[k + (int) offset]);
            }
            callBackFunction();
            return;
        }
        for (int k = 0; k < nElements; k++) {
            putString(src[k + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putStringArray(final String[] src, final long offset, final int[] dims) {
        final int nElements = putArraySizeDescriptor(dims);
        if (isEnforceSimpleStringEncoding()) {
            for (int k = 0; k < nElements; k++) {
                putStringISO8859(src[k + (int) offset]);
            }
            callBackFunction();
            return;
        }
        for (int k = 0; k < nElements; k++) {
            putString(src[k + (int) offset]);
        }
        callBackFunction();
    }

    @Override
    public void putStringISO8859(final String string) {
        final int strLength = string == null ? 0 : string.length();
        putInt(strLength + 1); // for C++ zero terminated string$
        for (int i = 0; i < strLength; ++i) {
            putByte((byte) (string.charAt(i) & 0xFF)); // ISO-8859-1 encoding
        }
        putByte((byte) 0); // For C++ zero terminated string
        callBackFunction();
    }

    @Override
    public long remaining() {
        return nioByteBuffer.remaining();
    }

    @Override
    public void reset() {
        nioByteBuffer.reset();
        nioByteBuffer.mark();
    }

    @Override
    public void setCallBackFunction(final Runnable runnable) {
        callBackFunction = runnable;
    }

    @Override
    public void setEnforceSimpleStringEncoding(final boolean state) {
        this.enforceSimpleStringEncoding = state;
    }

    @Override
    public void trim() {
        /* not implemented */
    }

    @Override
    public void trim(final int requestedCapacity) {
        /* not implemented */
    }

    private void callBackFunction() {
        if (callBackFunction != null) {
            callBackFunction.run();
        }
    }
}
