package de.gsi.dataset.serializer.spi.iobuffer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.gsi.dataset.serializer.FieldDescription;
import de.gsi.dataset.serializer.IoSerialiser;
import de.gsi.dataset.serializer.spi.AbstractSerialiser;
import de.gsi.dataset.serializer.spi.ClassDescriptions;
import de.gsi.dataset.serializer.spi.ClassFieldDescription;
import de.gsi.dataset.serializer.spi.FieldSerialiser;

/**
 * reference implementation for streaming arbitrary object to and from a IoBuffer-based byte-buffer
 *
 * @author rstein
 */
public class IoBufferSerialiser extends AbstractSerialiser {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoBufferSerialiser.class);
    private final Map<Integer, HashMap<Integer, ClassFieldDescription>> fieldToClassFieldDescription = new HashMap<>();

    /**
     * Initialises new IoBuffer-backed object serialiser
     *
     * @param ioSerialiser the backing IoSerialiser (see e.g. {@link de.gsi.dataset.serializer.IoSerialiser}
     * @see de.gsi.dataset.serializer.spi.BinarySerialiser
     */
    public IoBufferSerialiser(final IoSerialiser ioSerialiser) {
        super(ioSerialiser);
        startMarkerFunction = ioSerialiser::putStartMarker;
        endMarkerFunction = ioSerialiser::putEndMarker;

        // register primitive and boxed data type handlers
        FieldPrimitiveValueHelper.register(this);
        FieldPrimitveValueArrayHelper.register(this);
        FieldBoxedValueHelper.register(this);
        FieldBoxedValueArrayHelper.register(this);

        // Collection serialiser mapper to IoBuffer
        final FieldSerialiser.TriConsumer collectionReader = (io, obj, field) -> {
            final Collection<?> origCollection = (Collection<?>) field.getField().get(obj);
            origCollection.clear();

            final Collection<?> setVal = io.getCollection(origCollection);
            field.getField().set(obj, setVal);
        }; // reader
        final FieldSerialiser.TriFunction<Collection<?>> collectionReturn = (io, obj, field) -> {
            final Collection<?> origCollection = (Collection<?>) field.getField().get(obj);
            origCollection.clear();
            return io.getCollection(origCollection);
        }; // return function
        final FieldSerialiser.TriConsumer collectionWriter = (io, obj, field) -> io.put((Collection<?>) field.getField().get(obj)); // writer

        addClassDefinition(new FieldSerialiser<>(collectionReader, collectionReturn, collectionWriter, Collection.class));
        addClassDefinition(new FieldSerialiser<>(collectionReader, collectionReturn, collectionWriter, List.class));
        addClassDefinition(new FieldSerialiser<>(collectionReader, collectionReturn, collectionWriter, Queue.class));
        addClassDefinition(new FieldSerialiser<>(collectionReader, collectionReturn, collectionWriter, Set.class));

        // Enum serialiser mapper to IoBuffer
        addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> field.getField().set(obj, io.getEnum((Enum<?>) field.getField().get(obj))), // reader
                (io, obj, field) -> io.getEnum((Enum<?>) field.getField().get(obj)), // return
                (io, obj, field) -> io.put((Enum<?>) field.getField().get(obj)), // writer
                Enum.class));

        // Map serialiser mapper to IoBuffer
        addClassDefinition(new FieldSerialiser<>( //
                (io, obj, field) -> { // reader
                    final Map<?, ?> origMap = (Map<?, ?>) field.getField().get(obj);
                    origMap.clear();
                    final Map<?, ?> setVal = io.getMap(origMap);
                    field.getField().set(obj, setVal);
                }, // reader
                (io, obj, field) -> { // reader
                    final Map<?, ?> origMap = (Map<?, ?>) field.getField().get(obj);
                    origMap.clear();
                    return io.getMap(origMap);
                }, // return
                (io, obj, field) -> io.put((Map<?, ?>) field.getField().get(obj)), // writer
                Map.class));

        FieldDataSetHelper.register(this);
    }

    protected void deserialise(final Object obj, final FieldDescription fieldRoot, final ClassFieldDescription classField, final int recursionDepth) {
        if (classField.getFieldSerialiser() != null) {
            ioSerialiser.getBuffer().position(fieldRoot.getDataStartPosition());
            classField.getFieldSerialiser().getReaderFunction().accept(ioSerialiser, obj, classField);
            return;
        }

        if (fieldRoot.getFieldNameHashCode() != classField.getFieldNameHashCode() /*|| !fieldRoot.getFieldName().equals(classField.getFieldName())*/) {
            // did not find matching (sub-)field in class
            if (fieldRoot.getChildren().isEmpty()) {
                return;
            }
            // check for potential inner fields
            for (final FieldDescription fieldDescription : fieldRoot.getChildren()) {
                Map<Integer, ClassFieldDescription> rMap = fieldToClassFieldDescription.computeIfAbsent(recursionDepth, depth -> new HashMap<>());
                final ClassFieldDescription subFieldDescription = rMap.computeIfAbsent(fieldDescription.getFieldNameHashCode(), fieldNameHashCode -> (ClassFieldDescription) classField.findChildField(fieldNameHashCode, fieldDescription.getFieldName()));

                if (subFieldDescription != null) {
                    deserialise(obj, fieldDescription, subFieldDescription, recursionDepth + 1);
                }
            }
            return;
        }

        final Class<?> fieldClass = classField.getType();
        if (classField.isFinal() && !fieldClass.isInterface()) {
            // cannot set final variables
            LOGGER.atWarn().addArgument(classField.getParent()).addArgument(classField.getFieldName()).log("cannot (read: better should not) set final field '{}-{}'");
            return;
        }

        final FieldSerialiser<?> existingSerialiser = classField.getFieldSerialiser();
        final FieldSerialiser<?> fieldSerialiser = existingSerialiser == null ? cacheFindFieldSerialiser(classField.getType(), classField.getActualTypeArguments()) : existingSerialiser;

        if (fieldSerialiser == null) {
            final Object ref = classField.getField() == null ? obj : classField.getField().get(obj);
            final Object subRef;
            if (ref == null) {
                subRef = classField.allocateMemberClassField(obj);
            } else {
                subRef = ref;
            }

            // no specific deserialiser present check for potential inner fields
            for (final FieldDescription fieldDescription : fieldRoot.getChildren()) {
                Map<Integer, ClassFieldDescription> rMap = fieldToClassFieldDescription.computeIfAbsent(recursionDepth, depth -> new HashMap<>());
                final ClassFieldDescription subFieldDescription = rMap.computeIfAbsent(fieldDescription.getFieldNameHashCode(), fieldNameHashCode -> (ClassFieldDescription) classField.findChildField(fieldNameHashCode, fieldDescription.getFieldName()));

                if (subFieldDescription != null) {
                    deserialise(subRef, fieldDescription, subFieldDescription, recursionDepth + 1);
                }
            }
            return;
        }

        classField.setFieldSerialiser(fieldSerialiser);
        ioSerialiser.getBuffer().position(fieldRoot.getDataStartPosition());
        fieldSerialiser.getReaderFunction().accept(ioSerialiser, obj, classField);
    }

    @Override
    public Object deserialiseObject(final Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("obj must not be null (yet)");
        }

        final int startPosition = ioSerialiser.getBuffer().position();

        // match field header with class field description
        final ClassFieldDescription clazz = ClassDescriptions.get(obj.getClass());
        final FieldSerialiser<?> existingSerialiser = clazz.getFieldSerialiser();
        final FieldSerialiser<?> fieldSerialiser = existingSerialiser == null ? cacheFindFieldSerialiser(clazz.getType(), clazz.getActualTypeArguments()) : existingSerialiser;

        if (clazz.getFieldSerialiser() == null && fieldSerialiser != null) {
            clazz.setFieldSerialiser(fieldSerialiser);
        }

        ioSerialiser.getBuffer().position(startPosition);
        final FieldDescription fieldRoot = ioSerialiser.parseIoStream(true);

        if (fieldSerialiser != null) {
            // return new object
            final FieldDescription rawObjectFieldDescription = fieldRoot.getChildren().get(0).getChildren().get(0);
            ioSerialiser.getBuffer().position(rawObjectFieldDescription.getDataStartPosition());
            return clazz.getFieldSerialiser().getReturnObjectFunction().apply(ioSerialiser, obj, clazz);
        }
        // deserialise into object
        for (final FieldDescription child : fieldRoot.getChildren()) {
            deserialise(obj, child, clazz, 0);
        }

        return obj;
    }

    public void serialiseObject(final Object obj) {
        final ClassFieldDescription classField = ClassDescriptions.get(obj.getClass());
        final FieldSerialiser<?> existingSerialiser = classField.getFieldSerialiser();
        final FieldSerialiser<?> fieldSerialiser = existingSerialiser == null ? cacheFindFieldSerialiser(classField.getType(), classField.getActualTypeArguments()) : existingSerialiser;

        if (fieldSerialiser == null) {
            ioSerialiser.putHeaderInfo(classField);
            serialiseObject(obj, classField, 0);
            ioSerialiser.putEndMarker(classField.getFieldName());
        } else {
            if (classField.getFieldSerialiser() == null) {
                classField.setFieldSerialiser(fieldSerialiser);
            }
            ioSerialiser.putHeaderInfo();
            ioSerialiser.putCustomData(ioSerialiser, classField, obj, fieldSerialiser);
            ioSerialiser.putEndMarker("OBJ_ROOT_END");
        }
    }
}
