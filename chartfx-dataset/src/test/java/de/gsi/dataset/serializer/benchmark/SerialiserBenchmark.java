package de.gsi.dataset.serializer.benchmark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.flatbuffers.ArrayReadWriteBuf;
import com.google.flatbuffers.FlexBuffersBuilder;

import de.gsi.dataset.serializer.IoBuffer;
import de.gsi.dataset.serializer.helper.FlatBuffersHelper;
import de.gsi.dataset.serializer.helper.SerialiserHelper;
import de.gsi.dataset.serializer.helper.TestDataClass;
import de.gsi.dataset.serializer.spi.BinarySerialiser;
import de.gsi.dataset.serializer.spi.FastByteBuffer;
import de.gsi.dataset.serializer.spi.iobuffer.IoBufferSerialiser;

//import cern.cmw.data.Data;
//import cern.cmw.data.DataFactory;
//import cern.cmw.data.DataSerializer;

public class SerialiserBenchmark { // NOPMD - nomen est omen
    private static final Logger LOGGER = LoggerFactory.getLogger(SerialiserBenchmark.class);
    // private static final DataSerializer cmwSerializer = DataFactory.createDataSerializer();
    // private static final Data sourceData = createData();
    private static final IoBuffer byteBuffer = new FastByteBuffer(20000);
    // private static final IoBuffer byteBuffer = new ByteBuffer(20000);
    private static final BinarySerialiser binarySerialiser = new BinarySerialiser(byteBuffer);
    private static final IoBufferSerialiser ioSerialiser = new IoBufferSerialiser(binarySerialiser);
    private static final TestDataClass inputObject = new TestDataClass(10, 100, 1);
    private static TestDataClass outputObject = new TestDataClass(-1, -1, 0);
    private static byte[] rawByteBuffer = new byte[20000];
    private static int nBytesCMW;
    private static int nBytesIO;
    private static int nBytesFlatBuffers;

    //    public static void checkCMWIdentity() {
    //        final byte[] buffer = cmwSerializer.serializeToBinary(sourceData);
    //        final Data retrievedData = cmwSerializer.deserializeFromBinary(buffer);
    //        nBytesCMW = buffer.length;
    //
    //        LOGGER.atDebug().addArgument(compareData(sourceData, retrievedData)).log("compare = {}");
    //    }

    public static void checkIoBufferSerialiserIdentity() {
        byteBuffer.reset();

        ioSerialiser.serialiseObject(inputObject);

        // SerialiserHelper.serialiseCustom(byteBuffer, inputObject);
        nBytesIO = byteBuffer.position();
        LOGGER.atInfo().addArgument(nBytesIO).log("custom serialiser nBytes = {}");

        // keep: checks serialised data structure
        // byteBuffer.reset();
        // final WireDataFieldDescription fieldRoot = SerialiserHelper.deserialiseMap(byteBuffer);
        // fieldRoot.printFieldStructure();

        byteBuffer.reset();
        outputObject = (TestDataClass) ioSerialiser.deserialiseObject(outputObject);

        // second test - both vectors should have the same initial values after serialise/deserialise
        assertArrayEquals(inputObject.stringArray, outputObject.stringArray);

        assertEquals(inputObject, outputObject, "TestDataClass input-output equality");
    }

    public static void checkCustomSerialiserIdentity() {
        byteBuffer.reset();
        SerialiserHelper.serialiseCustom(binarySerialiser, inputObject);
        nBytesIO = byteBuffer.position();
        LOGGER.atInfo().addArgument(nBytesIO).log("custom serialiser nBytes = {}");

        // keep: checks serialised data structure
        // byteBuffer.reset();
        // final WireDataFieldDescription fieldRoot = SerialiserHelper.deserialiseMap(byteBuffer);
        // fieldRoot.printFieldStructure();

        byteBuffer.reset();
        SerialiserHelper.deserialiseCustom(binarySerialiser, outputObject);

        // second test - both vectors should have the same initial values after serialise/deserialise
        assertArrayEquals(inputObject.stringArray, outputObject.stringArray);

        assertEquals(inputObject, outputObject, "TestDataClass input-output equality");
    }

    public static void checkFlatBufferSerialiserIdentity() {
        //final FlexBuffersBuilder floatBuffersBuilder = new FlexBuffersBuilder(new ArrayReadWriteBuf(rawByteBuffer), FlexBuffersBuilder.BUILDER_FLAG_SHARE_KEYS_AND_STRINGS);
        final FlexBuffersBuilder floatBuffersBuilder = new FlexBuffersBuilder(new ArrayReadWriteBuf(rawByteBuffer), FlexBuffersBuilder.BUILDER_FLAG_NONE);
        final ByteBuffer retVal = FlatBuffersHelper.serialiseCustom(floatBuffersBuilder, inputObject);
        nBytesFlatBuffers = retVal.limit();
        LOGGER.atInfo().addArgument(nBytesFlatBuffers).log("flatBuffers serialiser nBytes = {}");
        FlatBuffersHelper.deserialiseCustom(retVal, outputObject);

        // second test - both vectors should have the same initial values after serialise/deserialise
        //        assertArrayEquals(inputObject.stringArray, outputObject.stringArray);

        assertEquals(inputObject, outputObject, "TestDataClass input-output equality");
    }

    public static String humanReadableByteCount(final long bytes, final boolean si) {
        final int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }

        final int exp = (int) (Math.log(bytes) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void main(final String... argv) {
        //        checkCMWIdentity();
        checkCustomSerialiserIdentity();
        checkIoBufferSerialiserIdentity();
        checkFlatBufferSerialiserIdentity();
        LOGGER.atInfo().addArgument(nBytesCMW).addArgument(nBytesIO).addArgument(nBytesFlatBuffers).log("bytes CMW: {} bytes IO: {} bytes FlatBuffers: {}");
        // binarySerialiser.setEnforceSimpleStringEncoding(true);
        binarySerialiser.setPutFieldMetaData(false);

        final int nIterations = 100000;
        for (int i = 0; i < 10; i++) {
            LOGGER.atInfo().addArgument(i).log("run {}");
            testIoSerialiserPerformanceMap(nIterations);
            // testCMWPerformanceMap(nIterations);
            testCustomIoSerialiserPerformance(nIterations);
            testFlatBuffersSerialiserPerformance(nIterations);
            // testCMWPerformancePojo(nIterations);
            testIoSerialiserPerformancePojo(nIterations);
        }
    }

    //    public static void testCMWPerformanceMap(final int iterations) {
    //        final long startTime = System.nanoTime();
    //
    //        byte[] buffer = new byte[0];
    //        for (int i = 0; i < iterations; i++) {
    //            buffer = cmwSerializer.serializeToBinary(sourceData);
    //            final Data retrievedData = cmwSerializer.deserializeFromBinary(buffer);
    //            if (sourceData.size() != retrievedData.size()) {
    //                // check necessary so that the above is not optimised by the Java JIT compiler
    //                // to NOP
    //                throw new IllegalStateException("data mismatch");
    //            }
    //        }
    //        final long stopTime = System.nanoTime();
    //
    //        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
    //        final double byteCount = iterations * ((buffer.length / diffMillis) * 1e3);
    //        LOGGER.atInfo().addArgument(humanReadableByteCount((long) byteCount, true)) //
    //                .addArgument(humanReadableByteCount((long) buffer.length, true))
    //                .addArgument(diffMillis) //
    //                .log("CMW Serializer (Map only) throughput = {}/s for {} per test run (took {} ms)");
    //    }

    //    public static void testCMWPerformancePojo(final int iterations) {
    //        final long startTime = System.nanoTime();
    //
    //        byte[] buffer = new byte[0];
    //        for (int i = 0; i < iterations; i++) {
    //            buffer = cmwSerializer.serializeToBinary(CmwHelper.getCmwData(inputObject));
    //            final Data retrievedData = cmwSerializer.deserializeFromBinary(buffer);
    //            CmwHelper.applyCmwData(retrievedData, outputObject);
    //            if (!inputObject.string1.contentEquals(outputObject.string1)) {
    //                // check necessary so that the above is not optimised by the Java JIT compiler to NOP
    //                throw new IllegalStateException("data mismatch");
    //            }
    //        }
    //        final long stopTime = System.nanoTime();
    //
    //        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
    //        final double byteCount = iterations * ((buffer.length / diffMillis) * 1e3);
    //        LOGGER.atInfo().addArgument(humanReadableByteCount((long) byteCount, true)) //
    //                .addArgument(humanReadableByteCount((long) buffer.length, true))
    //                .addArgument(diffMillis) //
    //                .log("CMW Serializer (POJO) throughput = {}/s for {} per test run (took {} ms)");
    //    }

    public static void testIoSerialiserPerformanceMap(final int iterations) {
        final long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            byteBuffer.reset();
            SerialiserHelper.serialiseCustom(binarySerialiser, inputObject);
            byteBuffer.reset();
            SerialiserHelper.deserialiseMap(binarySerialiser);

            if (!inputObject.string1.contentEquals(outputObject.string1)) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }

        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((byteBuffer.position() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(humanReadableByteCount((long) byteCount, true)) //
                .addArgument(humanReadableByteCount(byteBuffer.position(), true)) //
                .addArgument(diffMillis) //
                .log("IO Serializer (Map only)  throughput = {}/s for {} per test run (took {} ms)");
    }

    public static void testIoSerialiserPerformancePojo(final int iterations) {
        final long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            byteBuffer.reset();
            ioSerialiser.serialiseObject(inputObject);

            byteBuffer.reset();

            outputObject = (TestDataClass) ioSerialiser.deserialiseObject(outputObject);

            if (!inputObject.string1.contentEquals(outputObject.string1)) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }

        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((byteBuffer.position() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(humanReadableByteCount((long) byteCount, true)) //
                .addArgument(humanReadableByteCount(byteBuffer.position(), true)) //
                .addArgument(diffMillis) //
                .log("IO Serializer (POJO) throughput = {}/s for {} per test run (took {} ms)");
    }

    public static void testCustomIoSerialiserPerformance(final int iterations) {
        final long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            byteBuffer.reset();
            SerialiserHelper.serialiseCustom(binarySerialiser, inputObject);

            byteBuffer.reset();
            SerialiserHelper.deserialiseCustom(binarySerialiser, outputObject);

            if (!inputObject.string1.contentEquals(outputObject.string1)) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }

        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((byteBuffer.position() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(humanReadableByteCount((long) byteCount, true)) //
                .addArgument(humanReadableByteCount(byteBuffer.position(), true)) //
                .addArgument(diffMillis) //
                .log("IO Serializer (custom) throughput = {}/s for {} per test run (took {} ms)");
    }

    public static void testFlatBuffersSerialiserPerformance(final int iterations) {
        final long startTime = System.nanoTime();

        ByteBuffer retVal = FlatBuffersHelper.serialiseCustom(new FlexBuffersBuilder(new ArrayReadWriteBuf(rawByteBuffer), FlexBuffersBuilder.BUILDER_FLAG_SHARE_KEYS_AND_STRINGS), inputObject);
        for (int i = 0; i < iterations; i++) {
            //            retVal = FlatBuffersHelper.serialiseCustom(new FlexBuffersBuilder(new ArrayReadWriteBuf(rawByteBuffer), FlexBuffersBuilder.BUILDER_FLAG_SHARE_KEYS_AND_STRINGS), inputObject);
            retVal = FlatBuffersHelper.serialiseCustom(new FlexBuffersBuilder(new ArrayReadWriteBuf(rawByteBuffer), FlexBuffersBuilder.BUILDER_FLAG_NONE), inputObject);

            FlatBuffersHelper.deserialiseCustom(retVal, outputObject);

            if (!inputObject.string1.contentEquals(outputObject.string1)) {
                // quick check necessary so that the above is not optimised by the Java JIT compiler to NOP
                throw new IllegalStateException("data mismatch");
            }
        }

        final long stopTime = System.nanoTime();

        final double diffMillis = TimeUnit.NANOSECONDS.toMillis(stopTime - startTime);
        final double byteCount = iterations * ((retVal.limit() / diffMillis) * 1e3);
        LOGGER.atInfo().addArgument(humanReadableByteCount((long) byteCount, true)) //
                .addArgument(humanReadableByteCount(retVal.limit(), true)) //
                .addArgument(diffMillis) //
                .log("FlatBuffers (custom FlexBuffers) throughput = {}/s for {} per test run (took {} ms)");
    }
}
