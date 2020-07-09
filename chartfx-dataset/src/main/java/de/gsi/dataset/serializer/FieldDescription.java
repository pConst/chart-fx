package de.gsi.dataset.serializer;

import java.util.List;

public interface FieldDescription {
    int INDENTATION_NUMER_OF_SPACE = 4;

    FieldDescription findChildField(final int fieldNameHashCode, final String fieldName);

    List<FieldDescription> getChildren();

    long getDataSize();

    long getDataStartOffset();

    DataType getDataType();

    String getFieldName();

    String getFieldNameRelative();

    FieldDescription getParent();

    Class<?> getType();

    void printFieldStructure();
}
