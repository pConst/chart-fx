package de.gsi.dataset.serializer;

import java.util.List;

public interface FieldDescription {
    int INDENTATION_NUMER_OF_SPACE = 4;

    FieldDescription findChildField(final int fieldNameHashCode, final String fieldName);

    List<FieldDescription> getChildren();

    int getDataSize();

    int getDataStartOffset();

    int getDataStartPosition();

    DataType getDataType();

    String getFieldName();

    String getFieldNameRelative();

    int getFieldStart();

    FieldDescription getParent();

    Class<?> getType();

    int getFieldNameHashCode();

    void printFieldStructure();
}
