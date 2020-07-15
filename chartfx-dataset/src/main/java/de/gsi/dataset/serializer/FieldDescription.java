package de.gsi.dataset.serializer;

import java.util.List;

public interface FieldDescription {
    int INDENTATION_NUMER_OF_SPACE = 4;

    boolean isAnnotationPresent();

    FieldDescription findChildField(final int fieldNameHashCode, final String fieldName);

    List<FieldDescription> getChildren();

    int getDataSize();

    int getDataStartOffset();

    int getDataStartPosition();

    DataType getDataType();

    String getFieldDescription();

    String getFieldDirection();

    List<String> getFieldGroups();

    String getFieldName();

    int getFieldNameHashCode();

    int getFieldStart();

    String getFieldUnit();

    FieldDescription getParent();

    Class<?> getType();

    void printFieldStructure();
}
