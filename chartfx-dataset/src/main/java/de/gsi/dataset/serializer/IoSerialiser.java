package de.gsi.dataset.serializer;

import java.util.*;

import de.gsi.dataset.serializer.spi.ProtocolInfo;
import de.gsi.dataset.serializer.spi.WireDataFieldDescription;

public interface IoSerialiser {
    /**
     * Reads and checks protocol header information.
     * @return ProtocolInfo info Object (extends FieldHeader)
     * @throws IllegalStateException in case the format is incompatible with this serialiser
     */
    ProtocolInfo checkHeaderInfo();

    boolean isPutFieldMetaData();

    void setPutFieldMetaData(boolean putFieldMetaData);

    IoBuffer getBuffer();

    void setBuffer(final IoBuffer buffer);

    <E> Collection<E> getCollection(Collection<E> collection);

    <E extends Enum<E>> Enum<E> getEnum(Enum<E> enumeration);

    String getEnumTypeList();

    WireDataFieldDescription getFieldHeader();

    <E> List<E> getList(List<E> collection);

    <K, V> Map<K, V> getMap(Map<K, V> map);

    <E> Queue<E> getQueue(Queue<E> collection);

    <E> Set<E> getSet(Set<E> collection);

    WireDataFieldDescription parseIoStream(final boolean readHeader);

    <E> void put(Collection<E> collection);

    void put(Enum<?> enumeration);

    <K, V> void put(Map<K, V> map);

    void putEndMarker(String markerName);

    WireDataFieldDescription putFieldHeader(final FieldDescription fieldDescription);

    WireDataFieldDescription putFieldHeader(final String fieldName, DataType dataType);

    /**
     * Adds header and version information
     */
    void putHeaderInfo();

    void putStartMarker(String markerName);

    void updateDataEndMarker(WireDataFieldDescription fieldHeader, int... offset);
}
