package de.gsi.dataset.serializer.spi.iobuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import de.gsi.dataset.DataSet;
import de.gsi.dataset.serializer.IoBuffer;
import de.gsi.dataset.serializer.spi.BinarySerialiser;
import de.gsi.dataset.serializer.spi.ByteBuffer;
import de.gsi.dataset.serializer.spi.FastByteBuffer;
import de.gsi.dataset.spi.AbstractDataSet;
import de.gsi.dataset.spi.DefaultErrorDataSet;
import de.gsi.dataset.spi.DoubleDataSet;
import de.gsi.dataset.spi.DoubleErrorDataSet;
import de.gsi.dataset.spi.MultiDimDoubleDataSet;
import de.gsi.dataset.testdata.spi.TriangleFunction;

/**
 * @author Alexander Krimm
 */
public class DataSetSerialiserTests {
    private static final int BUFFER_SIZE = 10000;

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testDataSetFloatError(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(2 * BUFFER_SIZE);

        boolean asFloat32 = true;
        final DefaultErrorDataSet original = new DefaultErrorDataSet("test", new double[] { 1f, 2f, 3f },
                new double[] { 6f, 7f, 8f }, new double[] { 7f, 8f, 9f }, new double[] { 7f, 8f, 9f }, 3, false);
        addMetaData(original, true);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));

        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        buffer.reset(); // reset to read position (==0)
        final DataSet restored = ioSerialiser.readDataSetFromByteArray();

        assertEquals(original, restored);
    }

    private static void addMetaData(final AbstractDataSet<?> dataSet, final boolean addLabelsStyles) {
        if (addLabelsStyles) {
            dataSet.addDataLabel(1, "test");
            dataSet.addDataStyle(2, "color: red");
        }
        dataSet.getMetaInfo().put("Test", "Value");
        dataSet.getErrorList().add("TestError");
        dataSet.getWarningList().add("TestWarning");
        dataSet.getInfoList().add("TestInfo");
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testDataSetErrorSymmetric(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(BUFFER_SIZE);
        boolean asFloat32 = false;

        final DefaultErrorDataSet original = new DefaultErrorDataSet("test", new double[] { 1, 2, 3 },
                new double[] { 6, 7, 8 }, new double[] { 7, 8, 9 }, new double[] { 7, 8, 9 }, 3, false) {
            private static final long serialVersionUID = 1L;

            @Override
            public ErrorType getErrorType(int dimIndex) {
                if (dimIndex == 1) {
                    return ErrorType.SYMMETRIC;
                }
                return super.getErrorType(dimIndex);
            }
        };
        addMetaData(original, true);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));
        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        buffer.reset(); // reset to read position (==0)
        final DefaultErrorDataSet restored = (DefaultErrorDataSet) ioSerialiser.readDataSetFromByteArray();

        assertEquals(new DefaultErrorDataSet(original), new DefaultErrorDataSet(restored));
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testDataSetFloatErrorSymmetric(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(2 * BUFFER_SIZE);
        boolean asFloat32 = true;

        final DefaultErrorDataSet original = new DefaultErrorDataSet("test", new double[] { 1f, 2f, 3f },
                new double[] { 6f, 7f, 8f }, new double[] { 7f, 8f, 9f }, new double[] { 7f, 8f, 9f }, 3, false) {
            private static final long serialVersionUID = 1L;

            @Override
            public ErrorType getErrorType(int dimIndex) {
                if (dimIndex == 1) {
                    return ErrorType.SYMMETRIC;
                }
                return super.getErrorType(dimIndex);
            }
        };
        addMetaData(original, true);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));

        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        buffer.reset(); // reset to read position (==0)
        final DefaultErrorDataSet restored = (DefaultErrorDataSet) ioSerialiser.readDataSetFromByteArray();

        assertEquals(new DefaultErrorDataSet(original), new DefaultErrorDataSet(restored));
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testDataSet(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(2 * BUFFER_SIZE);

        boolean asFloat32 = false;
        final DoubleDataSet original = new DoubleDataSet(new TriangleFunction("test", 1009));
        addMetaData(original, true);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));

        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        buffer.reset(); // reset to read position (==0)
        final DataSet restored = ioSerialiser.readDataSetFromByteArray();

        assertEquals(original, restored);
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testErrorDataSet(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(10 * BUFFER_SIZE);

        boolean asFloat32 = false;
        final DoubleErrorDataSet original = new DoubleErrorDataSet(new TriangleFunction("test", 1009));
        addMetaData(original, true);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));

        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        buffer.reset(); // reset to read position (==0)
        final DataSet restored = ioSerialiser.readDataSetFromByteArray();

        assertEquals(original, restored);
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testMultiDimDataSet(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(2 * BUFFER_SIZE);

        boolean asFloat32 = false;
        final MultiDimDoubleDataSet original = new MultiDimDoubleDataSet("test", false,
                new double[][] { { 1, 2, 3 }, { 10, 20 }, { 0.5, 1, 1.5, 2, 2.5, 3 } });
        // Labels and styles are not correctly handled by multi dim data set because it is not really defined on which
        // dimension the label index is defined
        addMetaData(original, false);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));

        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        buffer.reset(); // reset to read position (==0)
        final DataSet restored = ioSerialiser.readDataSetFromByteArray();

        assertEquals(original, restored);
    }

    @ParameterizedTest(name = "IoBuffer class - {0}")
    @ValueSource(classes = { ByteBuffer.class, FastByteBuffer.class })
    public void testMultiDimDataSetFloatNoMetaDataAndLabels(final Class<? extends IoBuffer> bufferClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        assertNotNull(bufferClass, "bufferClass being not null");
        assertNotNull(bufferClass.getConstructor(int.class), "Constructor(Integer) present");
        final IoBuffer buffer = bufferClass.getConstructor(int.class).newInstance(2 * BUFFER_SIZE);

        boolean asFloat32 = true;
        final MultiDimDoubleDataSet original = new MultiDimDoubleDataSet("test", false,
                new double[][] { { 1, 2, 3 }, { 10, 20 }, { 0.5, 1, 1.5, 2, 2.5, 3 } });
        addMetaData(original, false);

        final DataSetSerialiser ioSerialiser = new DataSetSerialiser(new BinarySerialiser(buffer));

        ioSerialiser.setDataLablesSerialised(false);
        ioSerialiser.setMetaDataSerialised(false);
        ioSerialiser.writeDataSetToByteArray(original, asFloat32);
        ioSerialiser.setDataLablesSerialised(true);
        ioSerialiser.setMetaDataSerialised(true);
        buffer.reset(); // reset to read position (==0)
        final DataSet restored = ioSerialiser.readDataSetFromByteArray();

        MultiDimDoubleDataSet originalNoMetaData = new MultiDimDoubleDataSet(original);
        original.getMetaInfo().clear();
        original.getErrorList().clear();
        original.getWarningList().clear();
        original.getInfoList().clear();

        assertEquals(originalNoMetaData, restored);
    }
}
