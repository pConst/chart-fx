package de.gsi.dataset.serializer.spi.iobuffer;

import java.util.Collection;
import java.util.List;

import de.gsi.dataset.AxisDescription;
import de.gsi.dataset.serializer.IoSerialiser;
import de.gsi.dataset.serializer.spi.ClassFieldDescription;
import de.gsi.dataset.spi.DefaultAxisDescription;

/**
 * FieldSerialiser implementation for List&lt;AxisDescription&gt; to IoBuffer-backed byte-buffer
 * 
 * @author rstein
 */
public class FieldListAxisDescription extends IoBufferFieldSerialiser {
    /**
     * FieldSerialiser implementation for List&lt;AxisDescription&gt; to IoBuffer-backed byte-ioSerialiser
     * 
     * @param ioSerialiser the backing IoSerialiser
     * 
     */
    public FieldListAxisDescription(IoSerialiser ioSerialiser) {
        super(ioSerialiser, (obj, field) -> {}, (obj, field) -> {}, List.class, AxisDescription.class);
        readerFunction = this::execFieldReader;
        writerFunction = this::execFieldWriter;
    }

    protected final void execFieldReader(final Object obj, ClassFieldDescription field) {
        Collection<AxisDescription> setVal = (Collection<AxisDescription>) field.getField().get(obj); // NOPMD
        // N.B. cast should fail at runtime (points to lib inconsistency)
        setVal.clear();
        final int nElements = ioSerialiser.getBuffer().getInt(); // number of elements

        for (int i = 0; i < nElements; i++) {
            String axisName = ioSerialiser.getBuffer().getString();
            String axisUnit = ioSerialiser.getBuffer().getString();
            double min = ioSerialiser.getBuffer().getDouble();
            double max = ioSerialiser.getBuffer().getDouble();
            DefaultAxisDescription ad = new DefaultAxisDescription(i, axisName, axisUnit, min, max); // NOPMD
            // N.B. PMD - unavoidable in-loop instantiation
            setVal.add(ad);
        }

        field.getField().set(obj, setVal);
    }

    protected void execFieldWriter(Object obj, ClassFieldDescription field) {
        final List<AxisDescription> axisDescriptions = (List<AxisDescription>) field.getField().get(obj); // NOPMD
        // N.B. cast should fail at runtime (points to lib inconsistency)

        final int nElements = axisDescriptions.size();
        final int entrySize = 50; // as an initial estimate

        ioSerialiser.getBuffer().ensureAdditionalCapacity((nElements * entrySize) + 9L);
        ioSerialiser.getBuffer().putInt(nElements); // number of elements
        for (AxisDescription axis : axisDescriptions) {
            ioSerialiser.getBuffer().putString(axis.getName());
            ioSerialiser.getBuffer().putString(axis.getUnit());
            ioSerialiser.getBuffer().putDouble(axis.getMin());
            ioSerialiser.getBuffer().putDouble(axis.getMax());
        }
        ioSerialiser.updateDataEndMarker(null);
    }
}