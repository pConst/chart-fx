package de.gsi.dataset.serializer.spi;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.gsi.dataset.serializer.DataType;
import de.gsi.dataset.serializer.FieldDescription;
import de.gsi.dataset.serializer.IoBuffer;
import de.gsi.dataset.serializer.IoSerialiser;
import de.gsi.dataset.utils.AssertUtils;
import de.gsi.dataset.utils.GenericsHelper;

/**
 * Generic binary serialiser aimed at efficiently transferring data between server/client and in particular between
 * Java/C++/web-based programs.
 * 
 * <p>
 * There are two default backing buffer implementations ({@link de.gsi.dataset.serializer.spi.FastByteBuffer FastByteBuffer} and {@link de.gsi.dataset.serializer.spi.ByteBuffer ByteBuffer}), 
 * but can be extended/replaced with any other buffer is also possible provided it implements the {@link de.gsi.dataset.serializer.IoBuffer IoBuffer} interface.
 * 
 * <p>
 * The default serialisable data types are defined in {@link de.gsi.dataset.serializer.DataType DataType} and include definitions for 
 * <ul>
 * <li> primitives (byte, short, ..., float, double, and String), and 
 * <li> arrays thereof (ie. byte[], short[], ..., float[], double[], and String[]), as well as 
 * <li> complex objects implementing Collections (ie. Set, List, Queues), Enums or Maps.
 * </ul> 
 * Any other complex data objects can be stored/extended using the {@link DataType#OTHER OTHER} sub-type.
 * 
 * N.B. Multi-dimensional arrays are handled through one-dimensional striding arrays with the additional
 * infos on number of dimensions and size for each individual dimension.
 * 
 * <p>
 * <b>raw-byte level protocol</b>: above data items are stored as follows:
 * <pre><code>
 * * header info:   [ 4 bytes (int) = 0x0000002A] // magic number used as coarse protocol identifier - precise protocol refined by further fields below
 *                  [ clear text serialiser name: String ] + // e.g. "de.gsi.dataset.serializer.spi.BinarySerialiser"
 *                  [ 1 byte - major protocol version ] +
 *                  [ 1 byte - minor protocol version ] +
 *                  [ 1 byte - micro protocol version ] // micro: non API-changing bug fixes in implementation
 *                  [ field header for 'start marker' ] [ 1 byte - uniqueType (0x00) ]
 * * String:        [ 4 bytes (int) - length (including termination) ][ n bytes based on ISO-8859 or UTF-8 encoding ]
 * * field header:  # start field header 'p0'
 *                  [ 1 byte - uniqueType ]
 *                  [ 4 bytes - field name hash code] // enables faster field matching
 *                  [ 4 bytes - dataStart = n bytes until data start] // counted w.r.t. field header start
 *                  [ 4 bytes - dataSize = n bytes for data size]
 *                  [ String (ISO-8859) - field name ]             // optional, if there are no field name hash code collisions
 *                  N.B. following fields are optional (detectable if buffer position smaller than 'p0' + dataStart)
 *                  [ String (UTF-8)    - field unit ]
 *                  [ String (UTF-8)    - field in/out direction ]
 *                  [ String (UTF-8)    - field groups ]
 *                  # start data = 'p0' + dataStart
 *                  ... type specific and/or custom data serialisation
 *                  # end data = 'p0' + dataStart + dataSize
 * * primitives:    [ field header for 'primitive type ID'] + [ 1-8 bytes depending on DataType ]
 * * prim. arrays:  [ array header for 'prim. type array ID'] + [   ]=1-8 bytes x N_i or more - array data depending on variable DataType ]
 * * boxed arrays:  as above but each element cast to corresponding primitive type
 * * array header:  [ field header (as above) ] +
 *                      [4 bytes - number of dimensions N_d ] +
 *                      [4 bytes x N_d - vector sizes for each dimension N_i ]  
 * * Collection[E]:
 * * List[]:
 * * Queue[E]:
 * * Set[E]:        [ array header (uniqueType= one of the Collection type IDs) ] + 
 *                      [ 1 byte - uniqueType of E ] + [  n bytes - array of E cast to primitive type and/or string ]
 * * Map[K,V]:      [ array header (uniqueType=0xCB) ] + [ 1 byte - uniqueType of K ] +  [ 1 byte - uniqueType of V ] +
 *                      [ n bytes - array of K cast to primitive type and/or string ] + 
 *                      [ n bytes - array of V cast to primitive type and/or string ]
 * * OTHER          [ field header - uniqueByte = 0xFD ] +
 *                      [ 1 byte - uniqueType -- custom class type definition ]
 *                      [ String (ISO-8859) - class type name ]
 *                      [ n bytes - custom serialisation definition ]
 * * start marker:  [ field header for '0x00' ] // dataSize == # bytes until the corresponding end-marker start
 * * end marker:    [ field header for '0xFE' ]
 * 
 * * nesting or sub-structures (ie. POJOs with sub-classes) can be achieved via:
 * [  start marker - field name == nesting context1 ] 
 *   [  start marker - field name == nesting context2 ]
 *    ... 
 *   [  end marker - field name == nesting context2 (optional name) ]
 * [  end marker - field name == nesting context1 (optional name) ]
 * 
 * with
 * T: being a generic list parameter outlined in {@link de.gsi.dataset.serializer.DataType DataType}
 * K: being a generic key parameter outlined in {@link de.gsi.dataset.serializer.DataType DataType}
 * V: being a generic value parameter outlined in {@link de.gsi.dataset.serializer.DataType DataType}
 * </code></pre>
 * 
 * @author rstein
 */
@SuppressWarnings({ "PMD.CommentSize", "PMD.ExcessivePublicCount", "PMD.PrematureDeclaration", "unused" }) // variables need to be read from stream
public class BinarySerialiser implements IoSerialiser { // NOPMD - omen est omen
    private static final Logger LOGGER = LoggerFactory.getLogger(BinarySerialiser.class);
    public static final int VERSION_MAGIC_NUMBER = 0x0000002A;
    public static final byte VERSION_MAJOR = 1;
    public static final byte VERSION_MINOR = 0;
    public static final byte VERSION_MICRO = 0;
    private static final int ADDITIONAL_HEADER_INFO_SIZE = 1000;
    private static final DataType[] byteToDataType = new DataType[256];
    private static final Byte[] dataTypeToByte = new Byte[256];
    static {
        byteToDataType[0] = DataType.START_MARKER;

        byteToDataType[1] = DataType.BOOL;
        byteToDataType[2] = DataType.BYTE;
        byteToDataType[3] = DataType.SHORT;
        byteToDataType[4] = DataType.INT;
        byteToDataType[5] = DataType.LONG;
        byteToDataType[6] = DataType.FLOAT;
        byteToDataType[7] = DataType.DOUBLE;
        byteToDataType[8] = DataType.CHAR;
        byteToDataType[9] = DataType.STRING;

        byteToDataType[101] = DataType.BOOL_ARRAY;
        byteToDataType[102] = DataType.BYTE_ARRAY;
        byteToDataType[103] = DataType.SHORT_ARRAY;
        byteToDataType[104] = DataType.INT_ARRAY;
        byteToDataType[105] = DataType.LONG_ARRAY;
        byteToDataType[106] = DataType.FLOAT_ARRAY;
        byteToDataType[107] = DataType.DOUBLE_ARRAY;
        byteToDataType[108] = DataType.CHAR_ARRAY;
        byteToDataType[109] = DataType.STRING_ARRAY;

        byteToDataType[200] = DataType.COLLECTION;
        byteToDataType[201] = DataType.ENUM;
        byteToDataType[202] = DataType.LIST;
        byteToDataType[203] = DataType.MAP;
        byteToDataType[204] = DataType.QUEUE;
        byteToDataType[205] = DataType.SET;

        byteToDataType[0xFD] = DataType.OTHER;
        byteToDataType[0xFE] = DataType.END_MARKER;

        for (int i = 0; i < byteToDataType.length; i++) {
            if (byteToDataType[i] == null) {
                continue;
            }
            final int id = byteToDataType[i].getID();
            dataTypeToByte[id] = (byte) i;
        }
    }

    private int bufferIncrements = ADDITIONAL_HEADER_INFO_SIZE;
    private IoBuffer buffer;
    private boolean putFieldMetaData = true;
    private WireDataFieldDescription parent;
    private WireDataFieldDescription lastFieldHeader;
    private final Runnable callBackFunction = () -> updateDataEndMarker(lastFieldHeader);

    /**
     * @param buffer the backing IoBuffer (see e.g. {@link de.gsi.dataset.serializer.spi.FastByteBuffer} or{@link de.gsi.dataset.serializer.spi.ByteBuffer}
     */
    public BinarySerialiser(final IoBuffer buffer) {
        super();
        this.buffer = buffer;
        this.buffer.setCallBackFunction(callBackFunction);
    }

    @Override
    public ProtocolInfo checkHeaderInfo() {
        buffer.setCallBackFunction(null);
        final int magicNumber = buffer.getInt();
        final String producer = buffer.getString();
        final byte major = buffer.getByte();
        final byte minor = buffer.getByte();
        final byte micro = buffer.getByte();

        final WireDataFieldDescription headerStartField = getFieldHeader();
        final ProtocolInfo header = new ProtocolInfo(headerStartField, producer, major, minor, micro);

        if (magicNumber != VERSION_MAGIC_NUMBER) {
            throw new IllegalStateException("byte buffer version magic byte incompatible: received '" + magicNumber + "' vs. should '" + VERSION_MAGIC_NUMBER + "'");
        }

        if (!header.isCompatible()) {
            final String thisHeader = String.format(" serialiser: %s-v%d.%d.%d", BinarySerialiser.class.getCanonicalName(), VERSION_MAJOR, VERSION_MINOR, VERSION_MICRO);
            throw new IllegalStateException("byte buffer version incompatible: received '" + header.toString() + "' vs. should '" + thisHeader + "'");
        }
        buffer.setCallBackFunction(callBackFunction);
        return header;
    }

    public IoBuffer getBuffer() {
        return buffer;
    }

    public int getBufferIncrements() {
        return bufferIncrements;
    }

    @Override
    public <E> Collection<E> getCollection(final Collection<E> collection) {
        buffer.getArraySizeDescriptor();
        final int nElements = buffer.getInt();
        final DataType valueDataType = getDataType(buffer.getByte());
        // read value vector
        final Object[] values = getGenericArrayAsBoxedPrimitive(valueDataType);
        if (nElements != values.length) {
            throw new IllegalStateException("protocol mismatch nElements header = " + nElements + " vs. array = " + values.length);
        }

        final Collection<E> retCollection = collection == null ? new ArrayList<>(nElements) : collection;
        for (final Object value : values) {
            retCollection.add((E) value);
        }

        return retCollection;
    }

    @Override
    public <E extends Enum<E>> Enum<E> getEnum(final Enum<E> enumeration) {
        // read value vector
        final String enumSimpleName = buffer.getStringISO8859();
        final String enumName = buffer.getStringISO8859();
        buffer.getStringISO8859(); // enumTypeList
        final String enumState = buffer.getStringISO8859();
        buffer.getInt(); // enumOrdinal
        // TODO: implement matching by incomplete name match,
        // N.B. for the time being package name + class name is required
        Class<?> enumClass = ClassDescriptions.getClassByName(enumName);
        if (enumClass == null) {
            enumClass = ClassDescriptions.getClassByName(enumSimpleName);
            if (enumClass == null) {
                throw new IllegalStateException(
                        "could not find enum class description '" + enumName + "' or '" + enumSimpleName + "'");
            }
        }

        try {
            final Method valueOf = enumClass.getMethod("valueOf", String.class);
            return (Enum<E>) valueOf.invoke(null, enumState);
        } catch (final ReflectiveOperationException e) {
            LOGGER.atError().setCause(e).addArgument(enumClass).log("could not match 'valueOf(String)' function for class/(supposedly) enum of {}");
        }

        return null;
    }

    @Override
    public String getEnumTypeList() {
        // read value vector
        buffer.getStringISO8859(); // enumSimpleName
        buffer.getStringISO8859(); // enumName
        final String enumTypeList = buffer.getStringISO8859();
        buffer.getStringISO8859(); // enumState
        buffer.getInt(); // enumOrdinal

        return enumTypeList;
    }

    @Override
    public WireDataFieldDescription getFieldHeader() {
        final int headerStart = buffer.position();
        final byte dataTypeByte = buffer.getByte();
        final int fieldNameHashCode = buffer.getInt();
        final int dataStartOffset = buffer.getInt();
        final int dataStartPosition = headerStart + dataStartOffset;
        int dataSize = buffer.getInt();
        final String fieldName;
        if (buffer.position() < dataStartPosition) {
            fieldName = buffer.getStringISO8859();
        } else {
            fieldName = null;
        }
        final DataType dataType = getDataType(dataTypeByte);
        if (dataType == DataType.END_MARKER && parent != null && parent.getParent() != null) {
            parent = (WireDataFieldDescription) parent.getParent();
        }
        lastFieldHeader = new WireDataFieldDescription(parent, fieldNameHashCode, fieldName, dataType, headerStart, dataStartOffset, dataSize);
        if (dataType == DataType.START_MARKER) {
            parent = lastFieldHeader;
        }

        if (this.isPutFieldMetaData()) {
            // optional meta data
            if (buffer.position() < dataStartPosition) {
                lastFieldHeader.setFieldUnit(buffer.getString());
            }
            if (buffer.position() < dataStartPosition) {
                lastFieldHeader.setFieldDescription(buffer.getString());
            }
            if (buffer.position() < dataStartPosition) {
                lastFieldHeader.setFieldDirection(buffer.getString());
            }
            if (buffer.position() < dataStartPosition) {
                final String[] fieldGroups = buffer.getStringArray();
                lastFieldHeader.setFieldGroups(fieldGroups == null ? Collections.emptyList() : Arrays.asList(fieldGroups));
            }
        } else {
            buffer.position(dataStartPosition);
        }

        // check for header-dataStart offset consistency
        if (buffer.position() != dataStartPosition) {
            final int diff = dataStartPosition - buffer.position();
            throw new IllegalStateException("could not parse FieldHeader: fieldName='" + dataType + ":" + fieldName + "' dataOffset = " + dataStartOffset + " bytes (read) -- " //
                                            + " buffer position is " + buffer.position() + " vs. calculated " + dataStartPosition + " diff = " + diff);
        }

        if (dataSize >= 0) {
            return lastFieldHeader;
        }

        // last-minute check in case dataSize hasn't been set correctly
        if (dataType.isScalar()) {
            dataSize = dataType.getPrimitiveSize();
        } else if (dataType == DataType.STRING) {
            final int pos = buffer.position();
            dataSize = FastByteBuffer.SIZE_OF_INT + buffer.getInt(); // <(>string size -1> + <string byte data>
            buffer.position(pos);
        }
        lastFieldHeader.setDataSize(dataSize);

        return lastFieldHeader;
    }

    @Override
    public <E> List<E> getList(final List<E> collection) {
        buffer.getArraySizeDescriptor();
        final int nElements = buffer.getInt();
        final DataType valueDataType = getDataType(buffer.getByte());

        // read value vector
        final Object[] values = getGenericArrayAsBoxedPrimitive(valueDataType);
        if (nElements != values.length) {
            throw new IllegalStateException("protocol mismatch nElements header = " + nElements + " vs. array = " + values.length);
        }
        final List<E> retCollection = collection == null ? new ArrayList<>(nElements) : collection;
        for (final Object value : values) {
            retCollection.add((E) value);
        }

        return retCollection;
    }

    @Override
    public <K, V> Map<K, V> getMap(final Map<K, V> map) {
        buffer.getArraySizeDescriptor();
        buffer.getInt();
        // convert into two linear arrays one of K and the other for V streamer encoding as
        // <1 (int)><n_map (int)><type K (byte)<type V (byte)> <Length, [K_0,...,K_length]> <Length, [V_0, ..., V_length]>
        final DataType keyDataType = getDataType(buffer.getByte());
        final DataType valueDataType = getDataType(buffer.getByte());

        // read key and value vector
        final Object[] keys = getGenericArrayAsBoxedPrimitive(keyDataType);
        final Object[] values = getGenericArrayAsBoxedPrimitive(valueDataType);
        final Map<K, V> retMap = map == null ? new ConcurrentHashMap<>() : map;
        for (int i = 0; i < keys.length; i++) {
            retMap.put((K) keys[i], (V) values[i]);
        }

        return retMap;
    }

    @Override
    public WireDataFieldDescription getParent() {
        return parent;
    }

    @Override
    public <E> Queue<E> getQueue(final Queue<E> collection) {
        buffer.getArraySizeDescriptor();
        final int nElements = buffer.getInt();
        final DataType valueDataType = getDataType(buffer.getByte());

        // read value vector
        final Object[] values = getGenericArrayAsBoxedPrimitive(valueDataType);
        if (nElements != values.length) {
            throw new IllegalStateException("protocol mismatch nElements header = " + nElements + " vs. array = " + values.length);
        }
        final Queue<E> retCollection = collection == null ? new PriorityQueue<>(nElements) : collection;
        for (final Object value : values) {
            retCollection.add((E) value);
        }

        return retCollection;
    }

    @Override
    public <E> Set<E> getSet(final Set<E> collection) {
        buffer.getArraySizeDescriptor();
        final int nElements = buffer.getInt();
        final DataType valueDataType = getDataType(buffer.getByte());

        // read value vector
        final Object[] values = getGenericArrayAsBoxedPrimitive(valueDataType);
        if (nElements != values.length) {
            throw new IllegalStateException("protocol mismatch nElements header = " + nElements + " vs. array = " + values.length);
        }
        final Set<E> retCollection = collection == null ? new HashSet<>(nElements) : collection;
        for (final Object value : values) {
            retCollection.add((E) value);
        }

        return retCollection;
    }

    /**
     * @return {@code true} the ISO-8859-1 character encoding is being enforced for data fields (better performance), otherwise UTF-8 is being used (more generic encoding)
     */
    public boolean isEnforceSimpleStringEncoding() {
        return buffer.isEnforceSimpleStringEncoding();
    }

    @Override
    public boolean isPutFieldMetaData() {
        return putFieldMetaData;
    }

    @Override
    public WireDataFieldDescription parseIoStream(final boolean readHeader) {
        final WireDataFieldDescription fieldRoot = getRootElement();
        parent = fieldRoot;
        final WireDataFieldDescription headerRoot = readHeader ? checkHeaderInfo().getFieldHeader() : getFieldHeader();
        buffer.position(headerRoot.getDataStartPosition());
        parseIoStream(headerRoot, 0);
        updateDataEndMarker(fieldRoot);
        return fieldRoot;
    }

    public void parseIoStream(final WireDataFieldDescription fieldRoot, final int recursionDepth) {
        if (fieldRoot.getParent() == null) {
            parent = lastFieldHeader = fieldRoot;
        }
        WireDataFieldDescription field;
        while ((field = getFieldHeader()) != null) {
            final int dataSize = field.getDataSize();
            final int skipPosition = field.getDataStartPosition() + dataSize;

            if (field.getDataType() == DataType.END_MARKER) {
                // reached end of (sub-)class - close nested hierarchy
                break;
            }

            if (field.getDataType() == DataType.START_MARKER) {
                // detected sub-class start marker
                parseIoStream(field, recursionDepth + 1);
                continue;
            }

            if (dataSize < 0) {
                LOGGER.atWarn().addArgument(field.getFieldName()).addArgument(field.getDataType()).addArgument(dataSize).log("WireDataFieldDescription for '{}' type '{}' has bytesToSkip '{} <= 0'");
                // fall-back option in case of undefined dataSetSize -- usually indicated an internal serialiser error
                swallowRest(field);
            } else {
                buffer.position(skipPosition);
            }
        }
    }

    @Override
    public <E> void put(final Collection<E> collection, Class<?> genericType) {
        final Object[] values = collection.toArray();
        final int nElements = collection.size();
        final DataType valueDataType = nElements == 0 ? DataType.fromClassType(genericType) : DataType.fromClassType(values[0].getClass());
        final int entrySize = 17; // as an initial estimate

        buffer.putArraySizeDescriptor(nElements);
        buffer.ensureAdditionalCapacity((nElements * entrySize) + 9);
        buffer.putByte(getDataType(valueDataType)); // write value element type
        putGenericArrayAsPrimitive(valueDataType, values, 0, nElements);

        updateDataEndMarker(lastFieldHeader);
    }

    @Override
    public void put(final Enum<?> enumeration) {
        if (enumeration == null) {
            return;
        }
        final Class<? extends Enum<?>> clazz = (Class<? extends Enum<?>>) enumeration.getClass();
        if (clazz == null) {
            return;
        }
        final Enum<?>[] enumConsts = clazz.getEnumConstants();
        if (enumConsts == null) {
            return;
        }

        final int nElements = 1;
        final int entrySize = 17; // as an initial estimate

        buffer.ensureAdditionalCapacity((nElements * entrySize) + 9);
        final String typeList = Arrays.stream(clazz.getEnumConstants()).map(Object::toString).collect(Collectors.joining(", ", "[", "]"));
        buffer.putStringISO8859(clazz.getSimpleName());
        buffer.putStringISO8859(enumeration.getClass().getName());
        buffer.putStringISO8859(typeList);
        buffer.putStringISO8859(enumeration.name());
        buffer.putInt(enumeration.ordinal());

        updateDataEndMarker(lastFieldHeader);
    }

    @Override
    public <K, V> void put(final Map<K, V> map, Class<?> keyType, Class<?> valueType) {
        final Object[] keySet = map.keySet().toArray();
        final Object[] valueSet = map.values().toArray();
        final int nElements = keySet.length;
        buffer.putArraySizeDescriptor(nElements);
        final DataType keyDataType = nElements == 0 ? DataType.fromClassType(keyType) : DataType.fromClassType(keySet[0].getClass());
        final DataType valueDataType = nElements == 0 ? DataType.fromClassType(valueType) : DataType.fromClassType(valueSet[0].getClass());
        // convert into two linear arrays one of K and the other for V streamer encoding as
        // <1 (int)><n_map (int)><type K (byte)<type V (byte)> <Length, [K_0,...,K_length]> <Length, [V_0, ..., V_length]>
        final int entrySize = 17; // as an initial estimate

        buffer.ensureAdditionalCapacity((nElements * entrySize) + 9);
        buffer.putByte(getDataType(keyDataType)); // write key element type
        buffer.putByte(getDataType(valueDataType)); // write value element type
        putGenericArrayAsPrimitive(keyDataType, keySet, 0, nElements);
        putGenericArrayAsPrimitive(valueDataType, valueSet, 0, nElements);

        updateDataEndMarker(lastFieldHeader);
    }

    @Override
    public WireDataFieldDescription putCustomData(final IoSerialiser ioSerialiser, final FieldDescription fieldDescription, Object obj, FieldSerialiser<?> serialiser) {
        if (parent == null) {
            parent = lastFieldHeader = getRootElement();
        }
        final WireDataFieldDescription oldParent = parent;
        final WireDataFieldDescription ret = putFieldHeader(fieldDescription, DataType.OTHER);
        parent = lastFieldHeader;
        // buffer.putStringISO8859(fieldDescription.getType().getCanonicalName());
        serialiser.getWriterFunction().accept(ioSerialiser, obj, (ClassFieldDescription) fieldDescription);
        putEndMarker(fieldDescription.getFieldName());
        parent = oldParent;
        return ret;
    }

    @Override
    public void putEndMarker(final String markerName) {
        final WireDataFieldDescription oldParent = parent;
        updateDataEndMarker(oldParent);
        updateDataEndMarker(lastFieldHeader);
        if (parent != null && parent.getParent() != null) {
            parent = (WireDataFieldDescription) parent.getParent();
        }

        putFieldHeader(markerName, DataType.END_MARKER);
        buffer.putEndMarker(markerName);
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final FieldDescription fieldDescription) {
        return putFieldHeader(fieldDescription, fieldDescription.getDataType());
    }

    public WireDataFieldDescription putFieldHeader(final FieldDescription fieldDescription, DataType customDataType) {
        buffer.setCallBackFunction(null);
        if (isPutFieldMetaData()) {
            buffer.ensureAdditionalCapacity(bufferIncrements);
        }
        final boolean isScalar = customDataType.isScalar();

        // -- offset 0 vs. field start
        final int headerStart = buffer.position();
        buffer.putByte(getDataType(customDataType)); // data type ID
        buffer.putInt(fieldDescription.getFieldNameHashCode());
        buffer.putInt(-1); // dataStart offset
        final int dataSize = isScalar ? customDataType.getPrimitiveSize() : -1;
        buffer.putInt(dataSize); // dataSize (N.B. 'headerStart' + 'dataStart + dataSize' == start of next field header
        buffer.putStringISO8859(fieldDescription.getFieldName()); // full field name

        if (isPutFieldMetaData() && fieldDescription.isAnnotationPresent() && customDataType != DataType.END_MARKER) {
            buffer.putString(fieldDescription.getFieldUnit());
            buffer.putString(fieldDescription.getFieldDescription());
            buffer.putString(fieldDescription.getFieldDirection());
            buffer.putStringArray(fieldDescription.getFieldGroups().toArray(new String[0]));
        }

        // -- offset dataStart calculations
        final int fieldHeaderDataStart = buffer.position();
        final int dataStartOffset = fieldHeaderDataStart - headerStart;
        buffer.position(headerStart + 5);
        buffer.putInt(dataStartOffset); // write offset to dataStart
        buffer.position(fieldHeaderDataStart);
        buffer.setCallBackFunction(callBackFunction);

        // from hereon there are data specific structures
        buffer.ensureAdditionalCapacity(16); // allocate 16 bytes to account for potential array header (safe-bet)

        lastFieldHeader = new WireDataFieldDescription(parent, fieldDescription.getFieldNameHashCode(), fieldDescription.getFieldName(), customDataType, headerStart, dataStartOffset, dataSize);
        if (isPutFieldMetaData() && fieldDescription.isAnnotationPresent()) {
            lastFieldHeader.setFieldUnit(fieldDescription.getFieldUnit());
            lastFieldHeader.setFieldDescription(fieldDescription.getFieldDescription());
            lastFieldHeader.setFieldDirection(fieldDescription.getFieldDirection());
            lastFieldHeader.setFieldGroups(fieldDescription.getFieldGroups());
        }
        return lastFieldHeader;
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final String fieldName, final DataType dataType) {
        final int addCapacity = ((fieldName.length() + 18) * FastByteBuffer.SIZE_OF_BYTE) + bufferIncrements + dataType.getPrimitiveSize();
        buffer.setCallBackFunction(null);
        buffer.ensureAdditionalCapacity(addCapacity);
        final boolean isScalar = dataType.isScalar();

        // -- offset 0 vs. field start
        final int headerStart = buffer.position();
        buffer.putByte(getDataType(dataType)); // data type ID
        buffer.putInt(fieldName.hashCode()); // unique hashCode identifier -- TODO: unify across C++/Java & optimise performance
        buffer.putInt(-1); // dataStart offset
        final int dataSize = isScalar ? dataType.getPrimitiveSize() : -1;
        buffer.putInt(dataSize); // dataSize (N.B. 'headerStart' + 'dataStart + dataSize' == start of next field header
        buffer.putStringISO8859(fieldName); // full field name

        // this putField method cannot add meta-data use 'putFieldHeader(final FieldDescription fieldDescription)' instead

        // -- offset dataStart calculations
        final int fieldHeaderDataStart = buffer.position();
        final int dataStartOffset = (fieldHeaderDataStart - headerStart);
        buffer.position(headerStart + 5);
        buffer.putInt(dataStartOffset); // write offset to dataStart
        buffer.position(fieldHeaderDataStart);
        buffer.setCallBackFunction(callBackFunction);

        // from hereon there are data specific structures
        buffer.ensureAdditionalCapacity(16); // allocate 16 bytes to account for potential array header (safe-bet)

        lastFieldHeader = new WireDataFieldDescription(parent, fieldName.hashCode(), fieldName, dataType, headerStart, dataStartOffset, dataSize);
        return lastFieldHeader;
    }

    public void putGenericArrayAsPrimitive(final DataType dataType, final Object[] data, final int offset, final int nToCopy) {
        switch (dataType) {
        case BOOL:
            buffer.putBooleanArray(GenericsHelper.toBoolPrimitive(data), offset, nToCopy);
            break;
        case BYTE:
            buffer.putByteArray(GenericsHelper.toBytePrimitive(data), offset, nToCopy);
            break;
        case CHAR:
            buffer.putCharArray(GenericsHelper.toCharPrimitive(data), offset, nToCopy);
            break;
        case SHORT:
            buffer.putShortArray(GenericsHelper.toShortPrimitive(data), offset, nToCopy);
            break;
        case INT:
            buffer.putIntArray(GenericsHelper.toIntegerPrimitive(data), offset, nToCopy);
            break;
        case LONG:
            buffer.putLongArray(GenericsHelper.toLongPrimitive(data), offset, nToCopy);
            break;
        case FLOAT:
            buffer.putFloatArray(GenericsHelper.toFloatPrimitive(data), offset, nToCopy);
            break;
        case DOUBLE:
            buffer.putDoubleArray(GenericsHelper.toDoublePrimitive(data), offset, nToCopy);
            break;
        case STRING:
            buffer.putStringArray(GenericsHelper.toStringPrimitive(data), offset, nToCopy);
            break;
        case OTHER:
            break;
        default:
            throw new IllegalArgumentException("type not implemented - " + data[0].getClass().getSimpleName());
        }
    }

    @Override
    public void putHeaderInfo(final FieldDescription... field) {
        parent = lastFieldHeader = getRootElement();
        buffer.setCallBackFunction(null);

        buffer.ensureAdditionalCapacity(ADDITIONAL_HEADER_INFO_SIZE);
        buffer.putInt(VERSION_MAGIC_NUMBER);
        buffer.putStringISO8859(BinarySerialiser.class.getCanonicalName());
        buffer.putByte(VERSION_MAJOR);
        buffer.putByte(VERSION_MINOR);
        buffer.putByte(VERSION_MICRO);
        if (field.length == 0 || field[0] == null) {
            putStartMarker("OBJ_ROOT_START");
        } else {
            putStartMarker(field[0]);
        }
    }

    @Override
    public void putStartMarker(final String markerName) {
        putFieldHeader(markerName, DataType.START_MARKER);
        buffer.putStartMarker(markerName);
        parent = lastFieldHeader;
    }

    @Override
    public void putStartMarker(final FieldDescription fieldDescription) {
        putFieldHeader(fieldDescription);
        buffer.putStartMarker(fieldDescription.getFieldName());
        parent = lastFieldHeader;
    }

    public void setBuffer(final IoBuffer buffer) {
        this.buffer = buffer;
        this.buffer.setCallBackFunction(callBackFunction);
    }

    public void setBufferIncrements(final int bufferIncrements) {
        AssertUtils.gtEqThanZero("bufferIncrements", bufferIncrements);
        this.bufferIncrements = bufferIncrements;
    }

    /**
     *
     * @param state, {@code true} the ISO-8859-1 character encoding is being enforced for data fields (better performance), otherwise UTF-8 is being used (more generic encoding)
     */
    public void setEnforceSimpleStringEncoding(final boolean state) {
        buffer.setEnforceSimpleStringEncoding(state);
    }

    @Override
    public void setPutFieldMetaData(final boolean putFieldMetaData) {
        this.putFieldMetaData = putFieldMetaData;
    }

    @Override
    public void updateDataEndMarker(final WireDataFieldDescription fieldHeader) {
        final int sizeMarkerEnd = buffer.position();
        if (isPutFieldMetaData() && sizeMarkerEnd >= buffer.capacity()) {
            throw new IllegalStateException("buffer position " + sizeMarkerEnd + " is beyond buffer capacity " + buffer.capacity());
        }

        final int headerStart = fieldHeader.getFieldStart();
        final int dataStart = headerStart + fieldHeader.getDataStartOffset();
        final int dataSize = sizeMarkerEnd - dataStart;
        if (fieldHeader.getDataSize() != dataSize) {
            fieldHeader.setDataSize(dataSize);
            buffer.position(headerStart + 9); // 9 bytes = 1 byte for dataType, 4 bytes for fieldNameHashCode, 4 bytes for dataOffset
            buffer.putInt(dataSize);
            buffer.position(sizeMarkerEnd);
        }
    }

    protected Object[] getGenericArrayAsBoxedPrimitive(final DataType dataType) {
        final Object[] retVal;
        // @formatter:off
        switch (dataType) {
        case BOOL:
            retVal = GenericsHelper.toObject(buffer.getBooleanArray());
            break;
        case BYTE:
            retVal = GenericsHelper.toObject(buffer.getByteArray());
            break;
        case CHAR:
            retVal = GenericsHelper.toObject(buffer.getCharArray());
            break;
        case SHORT:
            retVal = GenericsHelper.toObject(buffer.getShortArray());
            break;
        case INT:
            retVal = GenericsHelper.toObject(buffer.getIntArray());
            break;
        case LONG:
            retVal = GenericsHelper.toObject(buffer.getLongArray());
            break;
        case FLOAT:
            retVal = GenericsHelper.toObject(buffer.getFloatArray());
            break;
        case DOUBLE:
            retVal = GenericsHelper.toObject(buffer.getDoubleArray());
            break;
        case STRING:
            retVal = buffer.getStringArray();
            break;
        case OTHER:
            retVal = new Object[0];
            break;
        // @formatter:on
        default:
            throw new IllegalArgumentException("type not implemented - " + dataType);
        }
        return retVal;
    }

    protected static int getNumberOfElements(final int[] dimensions) {
        AssertUtils.notNull("dimensions", dimensions);
        int ret = 1;
        for (int dim : dimensions) {
            ret *= dim;
        }
        return ret;
    }

    @SuppressWarnings("PMD.NcssCount")
    protected void swallowRest(final FieldDescription fieldDescription) {
        // parse whatever is left
        // N.B. this is/should be the only place where 'Object' is used since the JVM will perform boxing of primitive types
        // automatically. Boxing and later un-boxing is a significant high-performance bottleneck for any serialiser
        Object leftOver;
        int size = -1;
        switch (fieldDescription.getDataType()) {
        case BOOL:
            leftOver = buffer.getBoolean();
            break;
        case BYTE:
            leftOver = buffer.getByte();
            break;
        case SHORT:
            leftOver = buffer.getShort();
            break;
        case INT:
            leftOver = buffer.getInt();
            break;
        case LONG:
            leftOver = buffer.getLong();
            break;
        case FLOAT:
            leftOver = buffer.getFloat();
            break;
        case DOUBLE:
            leftOver = buffer.getDouble();
            break;
        case STRING:
            leftOver = buffer.getString();
            break;
        case BOOL_ARRAY:
            leftOver = buffer.getBooleanArray();
            break;
        case BYTE_ARRAY:
            leftOver = buffer.getByteArray();
            break;
        case SHORT_ARRAY:
            leftOver = buffer.getShortArray();
            break;
        case INT_ARRAY:
            leftOver = buffer.getIntArray();
            break;
        case LONG_ARRAY:
            leftOver = buffer.getLongArray();
            break;
        case FLOAT_ARRAY:
            leftOver = buffer.getFloatArray();
            break;
        case DOUBLE_ARRAY:
            leftOver = buffer.getDoubleArray();
            break;
        case STRING_ARRAY:
            leftOver = buffer.getStringArray();
            break;
        case COLLECTION:
            leftOver = getCollection(new ArrayList<>());
            break;
        case LIST:
            leftOver = getList(new ArrayList<>());
            break;
        case SET:
            leftOver = getSet(new HashSet<>());
            break;
        case QUEUE:
            leftOver = getQueue(new PriorityQueue<>());
            break;
        case MAP:
            leftOver = getMap(new ConcurrentHashMap<>());
            break;
        case ENUM:
            leftOver = getEnumTypeList();
            break;
        case START_MARKER:
        case END_MARKER:
            size = 1;
            leftOver = null;
            break;
        default:
            throw new IllegalArgumentException("encountered unknown format for " + fieldDescription.toString());
        }

        if (buffer.position() >= buffer.capacity()) {
            throw new IllegalStateException("read beyond buffer capacity, position = " + buffer.position() + " vs capacity = " + buffer.capacity());
        }

        LOGGER.atTrace().addArgument(fieldDescription).addArgument(leftOver).addArgument(size).log("swallowed unused element '{}'='{}' size = {}");
    }

    private WireDataFieldDescription getRootElement() {
        return new WireDataFieldDescription(null, "ROOT".hashCode(), "ROOT", DataType.OTHER, buffer.position(), -1, -1);
    }

    public static byte getDataType(final DataType dataType) {
        final int id = dataType.getID();
        if (dataTypeToByte[id] != null) {
            return dataTypeToByte[id];
        }

        throw new IllegalArgumentException("DataType " + dataType + " not mapped to specific byte");
    }

    public static DataType getDataType(final byte byteValue) {
        final int id = byteValue & 0xFF;
        if (dataTypeToByte[id] != null) {
            return byteToDataType[id];
        }

        throw new IllegalArgumentException("DataType byteValue=" + byteValue + " rawByteValue=" + (byteValue & 0xFF) + " not mapped");
    }
}
