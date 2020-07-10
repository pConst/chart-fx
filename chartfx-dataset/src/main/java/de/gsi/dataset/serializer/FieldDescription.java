package de.gsi.dataset.serializer;

import java.util.List;

public interface FieldDescription {
    int INDENTATION_NUMER_OF_SPACE = 4;

    FieldDescription findChildField(final int fieldNameHashCode, final String fieldName);

    List<FieldDescription> getChildren();

    long getDataSize();

    long getDataStartOffset();

    long getDataStartPosition();

    DataType getDataType();

    String getFieldName();

    String getFieldNameRelative();

    long getFieldStart();

    FieldDescription getParent();

    Class<?> getType();

    void printFieldStructure();
}
