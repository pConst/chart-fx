package de.gsi.dataset.serializer;

/**
 * Interface definition in line with the jdk Buffer abstract class. This definition is needed to allow for redirect or
 * different buffer implementations.
 *
 * @author rstein
 */
@SuppressWarnings("PMD.TooManyMethods") // NOPMD - these are short-hand convenience methods
public interface IoBuffer extends IoBufferHeader {
    int[] getArraySizeDescriptor();

    boolean getBoolean(); // NOPMD by rstein

    default boolean[] getBooleanArray() {
        return getBooleanArray(null, 0, 0);
    }

    default boolean[] getBooleanArray(final boolean[] dst) {
        return getBooleanArray(dst, 0, dst == null ? -1 : dst.length);
    }

    boolean[] getBooleanArray(final boolean[] dst, final long offset, final int length);

    byte getByte();

    default byte[] getByteArray() {
        return getByteArray(null, 0, 0);
    }

    default byte[] getByteArray(final byte[] dst) {
        return getByteArray(dst, 0, dst == null ? -1 : dst.length);
    }

    byte[] getByteArray(final byte[] dst, final long offset, final int length);

    char getChar();

    default char[] getCharArray() {
        return getCharArray(null, 0, 0);
    }

    default char[] getCharArray(final char[] dst) {
        return getCharArray(dst, 0, dst == null ? -1 : dst.length);
    }

    char[] getCharArray(final char[] dst, final long offset, final int length);

    double getDouble();

    default double[] getDoubleArray() {
        return getDoubleArray(null, 0, 0);
    }

    default double[] getDoubleArray(final double[] dst) {
        return getDoubleArray(dst, 0, dst == null ? -1 : dst.length);
    }

    double[] getDoubleArray(final double[] dst, final long offset, final int length);

    float getFloat();

    default float[] getFloatArray() {
        return getFloatArray(null, 0, 0);
    }

    default float[] getFloatArray(final float[] dst) {
        return getFloatArray(dst, 0, dst == null ? -1 : dst.length);
    }

    float[] getFloatArray(final float[] dst, final long offset, final int length);

    int getInt();

    default int[] getIntArray() {
        return getIntArray(null, 0, 0);
    }

    default int[] getIntArray(final int[] dst) {
        return getIntArray(dst, 0, dst == null ? -1 : dst.length);
    }

    int[] getIntArray(final int[] dst, final long offset, final int length);

    long getLong();

    default long[] getLongArray() {
        return getLongArray(null, 0, 0);
    }

    default long[] getLongArray(final long[] dst) {
        return getLongArray(dst, 0, dst == null ? -1 : dst.length);
    }

    long[] getLongArray(final long[] dst, final long offset, final int length);

    short getShort(); // NOPMD by rstein

    default short[] getShortArray() { // NOPMD by rstein
        return getShortArray(null, 0, 0);
    }

    default short[] getShortArray(final short[] dst) { // NOPMD by rstein
        return getShortArray(dst, 0, dst == null ? -1 : dst.length);
    }

    short[] getShortArray(final short[] dst, final long offset, final int length); // NOPMD by rstein

    String getString();

    default String[] getStringArray() {
        return getStringArray(null, 0, 0);
    }

    default String[] getStringArray(final String[] dst) {
        return getStringArray(dst, 0, dst == null ? -1 : dst.length);
    }

    String[] getStringArray(final String[] dst, final long offset, final int length);

    String getStringISO8859();

    /**
     * @return {@code true} the ISO-8859-1 character encoding is being enforced for data fields (better performance), otherwise UTF-8 is being used (more generic encoding)
     */
    boolean isEnforceSimpleStringEncoding();

    /**
     * @param state, {@code true} the ISO-8859-1 character encoding is being enforced for data fields (better performance), otherwise UTF-8 is being used (more generic encoding)
     */
    void setEnforceSimpleStringEncoding(boolean state);

    int putArraySizeDescriptor(int n);

    int putArraySizeDescriptor(int[] dims);

    void putBoolean(boolean value);

    void putBooleanArray(final boolean[] src, final long offset, final int n);

    void putBooleanArray(final boolean[] src, final long offset, final int[] dims);

    void putByte(final byte b);

    void putByteArray(final byte[] src, final long offset, final int n);

    void putByteArray(final byte[] src, final long offset, final int[] dims);

    void putChar(char value);

    void putCharArray(final char[] src, final long offset, final int n);

    void putCharArray(final char[] src, final long offset, final int[] dims);

    void putDouble(double value);

    void putDoubleArray(final double[] src, final long offset, final int n);

    void putDoubleArray(final double[] src, final long offset, final int[] dims);

    void putFloat(float value);

    void putFloatArray(final float[] src, final long offset, final int n);

    void putFloatArray(final float[] src, final long offset, final int[] dims);

    void putInt(int value);

    void putIntArray(final int[] src, final long offset, final int n);

    void putIntArray(final int[] src, final long offset, final int[] dims);

    void putLong(long value);

    void putLongArray(final long[] src, final long offset, final int n);

    void putLongArray(final long[] src, final long offset, final int[] dims);

    void putShort(short value); // NOPMD

    void putShortArray(final short[] src, final long offset, final int n); // NOPMD

    void putShortArray(final short[] src, final long offset, final int[] dims); // NOPMD

    void putString(String string);

    void putStringArray(final String[] src, final long offset, final int n); // NOPMD

    void putStringArray(final String[] src, final long offset, final int[] dims); // NOPMD

    void putStringISO8859(String string);
}
