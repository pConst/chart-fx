package de.gsi.dataset.serializer.spi;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.gsi.dataset.serializer.FieldDescription;
import de.gsi.dataset.serializer.IoSerialiser;

/**
 * @author rstein
 */
public abstract class AbstractSerialiser {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSerialiser.class);
    private static final Map<String, Constructor<Object>> CLASS_CONSTRUCTOR_MAP = new ConcurrentHashMap<>();
    private final Map<Class<?>, List<FieldSerialiser>> classMap = new ConcurrentHashMap<>();
    protected final IoSerialiser ioSerialiser;
    protected Consumer<String> startMarkerFunction;
    protected Consumer<String> endMarkerFunction;

    public AbstractSerialiser(final IoSerialiser ioSerialiser) {
        if (ioSerialiser == null) {
            throw new IllegalArgumentException("buffer must not be null");
        }
        this.ioSerialiser = ioSerialiser;
    }

    public void addClassDefinition(FieldSerialiser serialiser) {
        if (serialiser == null) {
            throw new IllegalArgumentException("serialiser must not be null");
        }
        if (serialiser.getClassPrototype() == null) {
            throw new IllegalArgumentException("clazz must not be null");
        }
        if (serialiser.getGenericsPrototypes() == null) {
            throw new IllegalArgumentException("types must not be null");
        }
        synchronized (knownClasses()) {
            final List<FieldSerialiser> list = knownClasses().computeIfAbsent(serialiser.getClassPrototype(),
                    key -> new ArrayList<>());

            if (list.isEmpty() || !list.contains(serialiser)) {
                list.add(serialiser);
            }
        }
    }

    protected boolean checkClassCompatibility(List<Class<?>> ref1, List<Class<?>> ref2) {
        if (ref1.size() != ref2.size()) {
            return false;
        }
        if (ref1.isEmpty() && ref2.isEmpty()) {
            return true;
        }

        for (int i = 0; i < ref1.size(); i++) {
            final Class<?> class1 = ref1.get(i);
            final Class<?> class2 = ref2.get(i);
            if (!class1.equals(class2) && !(class2.isAssignableFrom(class1))) {
                return false;
            }
        }

        return true;
    }

    public abstract Object deserialiseObject(final Object obj) throws IllegalAccessException;

    public FieldSerialiser findFieldSerialiserForKnownClassOrInterface(Class<?> clazz, List<Class<?>> classGenericArguments) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz must not be null");
        }
        final List<FieldSerialiser> directClassMatchList = classMap.get(clazz);
        if (directClassMatchList != null && !directClassMatchList.isEmpty()) {
            if (directClassMatchList.size() == 1 || classGenericArguments == null || classGenericArguments.isEmpty()) {
                return directClassMatchList.get(0);
            }
            // more than one possible serialiser implementation
            for (final FieldSerialiser entry : directClassMatchList) {
                if (checkClassCompatibility(classGenericArguments, entry.getGenericsPrototypes())) {
                    return entry;
                }
            }
            // found FieldSerialiser entry but not matching required generic types
        }

        // did not find FieldSerialiser entry by specific class -> search for assignable interface definitions

        // N.B. slower stream impl: final List<Class<?>> potentialMatchingKeys = this.knownClasses().keySet().stream().filter(k -> k.isAssignableFrom(clazz)).collect(Collectors.toList());
        final List<Class<?>> potentialMatchingKeys = new ArrayList<>(10);
        for (Class<?> testClass : knownClasses().keySet()) {
            if (testClass.isAssignableFrom(clazz)) {
                potentialMatchingKeys.add(testClass);
            }
        }
        if (potentialMatchingKeys.isEmpty()) {
            // did not find any matching clazz/interface FieldSerialiser entries
            return null;
        }

        // N.B. slower stream impl: final List<FieldSerialiser> interfaceMatchList = potentialMatchingKeys.stream().map(key -> this.knownClasses().get(key)).flatMap(List::stream).collect(Collectors.toList());
        final List<FieldSerialiser> interfaceMatchList = new ArrayList<>(10);
        for (Class<?> testClass : potentialMatchingKeys) {
            final List<FieldSerialiser> fieldSerialisers = knownClasses().get(testClass);
            if (fieldSerialisers.isEmpty()) {
                continue;
            }
            interfaceMatchList.addAll(fieldSerialisers);
        }
        if (interfaceMatchList.size() == 1 || classGenericArguments == null || classGenericArguments.isEmpty()) {
            // found single match FieldSerialiser entry type w/o specific generics requirements
            return interfaceMatchList.get(0);
        }

        // more than one possible serialiser implementation
        for (final FieldSerialiser entry : interfaceMatchList) {
            if (checkClassCompatibility(classGenericArguments, entry.getGenericsPrototypes())) {
                // found generics matching or assignable entry
                return entry;
            }
        }
        // could not match with generics arguments

        // find generic serialiser entry w/o generics parameter requirements
        return interfaceMatchList.stream().filter(entry -> entry.getGenericsPrototypes().isEmpty()).findFirst().get();
    }

    public Map<Class<?>, List<FieldSerialiser>> knownClasses() {
        return classMap;
    }

    public void serialiseObject(final Object obj) throws IllegalAccessException {
        serialiseObject(obj, ClassDescriptions.get(obj.getClass()), 0);
    }

    public void serialiseObject(final Object rootObj, final ClassFieldDescription classField, final int recursionDepth) throws IllegalAccessException {
        // final FieldSerialiser existingSerialiser = classField.getDataType().isScalar() ? classField.getFieldSerialiser() : null;
        final FieldSerialiser fieldSerialiser = findFieldSerialiserForKnownClassOrInterface(classField.getType(), classField.getActualTypeArguments());

        if (fieldSerialiser != null && recursionDepth != 0) {
            classField.setFieldSerialiser(fieldSerialiser);
            // write field header
            final WireDataFieldDescription header = ioSerialiser.putFieldHeader(classField);
            fieldSerialiser.getWriterFunction().exec(rootObj, classField);
            ioSerialiser.updateDataEndMarker(header);
            return;
        }
        // cannot serialise field check whether this is a container class and contains serialisable children

        if (classField.getChildren().isEmpty()) {
            // no further children
            return;
        }

        // dive into it's children
        final String subClass = classField.getFieldName();
        if (recursionDepth != 0 && startMarkerFunction != null) {
            startMarkerFunction.accept(subClass);
        }

        final Object newRoot = classField.getField() == null ? rootObj : classField.getField().get(rootObj);
        for (final FieldDescription fieldDescription : classField.getChildren()) {
            ClassFieldDescription field = (ClassFieldDescription) fieldDescription;

            if (!field.isPrimitive()) {
                final Object reference = field.getField().get(newRoot);
                if (!field.isPrimitive() && reference == null) {
                    // only follow and serialise non-null references of sub-classes
                    continue;
                }
            }
            serialiseObject(newRoot, field, recursionDepth + 1);
        }

        if (recursionDepth != 0 && endMarkerFunction != null) {
            endMarkerFunction.accept(subClass);
        }
    }

    public static int computeHashCode(final Class<?> classPrototype, List<Class<?>> classGenericArguments) {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((classPrototype == null) ? 0 : classPrototype.getName().hashCode());
        if (classGenericArguments == null || classGenericArguments.isEmpty()) {
            return result;
        }
        for (final Type arg : classGenericArguments) {
            result = prime * result + ((arg == null) ? 0 : arg.getTypeName().hashCode());
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public static Constructor<Object> getClassConstructorByName(final String name, Class<?>... parameterTypes) {
        return CLASS_CONSTRUCTOR_MAP.computeIfAbsent(name, key -> {
            try {
                return (Constructor<Object>) ClassDescriptions.getClassByName(key)
                        .getDeclaredConstructor(parameterTypes);
            } catch (SecurityException | NoSuchMethodException e) {
                LOGGER.atError().setCause(e).addArgument(Arrays.toString(parameterTypes)).addArgument(name).log("exception while getting constructor{} for class {}");
                return null;
            }
        });
    }

    public static String[] getClassNames(List<Class<?>> classGenericArguments) {
        if (classGenericArguments == null) {
            return new String[0];
        }
        final String[] argStrings = new String[classGenericArguments.size()];
        for (int i = 0; i < argStrings.length; i++) {
            argStrings[i] = classGenericArguments.get(i).getName();
        }
        return argStrings;
    }

    public static String getGenericFieldSimpleTypeString(List<Class<?>> classArguments) {
        if (classArguments == null || classArguments.isEmpty()) {
            return "";
        }
        return classArguments.stream().map(Class::getSimpleName).collect(Collectors.joining(", ", "<", ">"));
    }
}
