package de.gsi.dataset.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static de.gsi.dataset.DataSet.DIM_X;
import static de.gsi.dataset.DataSet.DIM_Y;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import de.gsi.dataset.DataSet;
import de.gsi.dataset.DataSetError.ErrorType;
import de.gsi.dataset.EditConstraints;
import de.gsi.dataset.utils.AssertUtils;

/**
 * Tests for minimal DataSet equality and hashCode implementation
 *
 * @author rstein
 */
public class DataSetEqualityTests {
    @Test
    public void testDataSetEquality() {
        // check for helper classes
        assertEquals(new DataRange(), new DataRange());
        assertEquals(new DefaultAxisDescription(DIM_X, "default"), new DefaultAxisDescription(DIM_X, "default"));

        // common DataSet implementations
        assertEquals(new DoubleDataSet("default"), new DoubleDataSet("default"));
        assertEquals(new DoubleErrorDataSet("default"), new DoubleErrorDataSet("default"));
        assertEquals(new FloatDataSet("default"), new FloatDataSet("default"));

        assertEquals(new CircularDoubleErrorDataSet("default", 10), new CircularDoubleErrorDataSet("default", 11));

        assertEquals(new DefaultDataSet("default"), new DefaultDataSet("default"));
        assertEquals(new DefaultErrorDataSet("default"), new DefaultErrorDataSet("default"));
        assertEquals(new DoubleDataSet("default"), new DoubleDataSet("default"));
        assertEquals(new DoubleErrorDataSet("default"), new DoubleErrorDataSet("default"));
        assertEquals(new FifoDoubleErrorDataSet("default", 10), new FifoDoubleErrorDataSet("default", 11));
        assertEquals(new FragmentedDataSet("default"), new FragmentedDataSet("default"));
        assertEquals(new Histogram("default", 10, 0.0, 1.0), new Histogram("default", 10, 0.0, 1.0));
        // assertEquals(new Histogram2("default", 10, 0.0, 1.0, 10, 0.0, 1.0),
        // new Histogram2("default", 10, 0.0, 1.0, 10, 0.0, 1.0));
        assertEquals(new LabelledMarkerDataSet("default"), new LabelledMarkerDataSet("default"));
        assertEquals(new LimitedIndexedTreeDataSet("default", 10), new LimitedIndexedTreeDataSet("default", 11));
        assertEquals(new RollingDataSet("default"), new RollingDataSet("default"));
        assertEquals(new WrappedDataSet("default"), new WrappedDataSet("default"));

        assertEquals(new LimitedIndexedTreeDataSet("default", 100), new LimitedIndexedTreeDataSet("default", 100));
    }

    /**
     * more specific test here DoubleErrorDataSet as stand-in for all AbstractDataSet derived classes
     */
    @Test
    public void testDoubleDataSetEquality() {
        final DoubleErrorDataSet ds1 = new DoubleErrorDataSet("default");
        final DoubleErrorDataSet ds2 = new DoubleErrorDataSet("default");

        assertEquals(ds1, ds1);
        assertNotEquals(null, ds1);
        assertNotEquals(ds1, new OneDimDataSet());
        assertNotEquals("", ds1);
        assertEquals(ds1, ds2);

        ds1.setName(null);
        assertNotEquals(ds1, ds2);
        ds2.setName(null);
        assertEquals(ds1, ds2);
        ds1.setName("different Name");
        assertNotEquals(ds1, ds2);
        ds2.setName("different Name");
        assertEquals(ds1, ds2);
        ds2.setName(null);
        assertNotEquals(ds1, ds2);
        ds2.setName("different Name");

        // check equality of normal X/Y values
        ds1.add(0.0, 1.0);
        assertNotEquals(ds1, ds2);
        ds2.add(0.0, 1.0);
        assertEquals(ds1, ds2);
        ds2.set(0, 0.0, 1.5);
        assertNotEquals(ds1, ds2);
        ds1.set(0, 0.0, 1.5);
        assertEquals(ds1, ds2);
        ds2.set(0, 1.0, 1.5);
        assertNotEquals(ds1, ds2);
        ds1.set(0, 1.0, 1.5);
        assertEquals(ds1, ds2);

        // check equality of X/Y error values
        ds1.setErrorType(DIM_X, ErrorType.NO_ERROR);
        ds1.setErrorType(DIM_Y, ErrorType.NO_ERROR);
        assertNotEquals(ds1, ds2);
        ds1.setErrorType(DIM_Y, ErrorType.ASYMMETRIC);
        assertEquals(ds1, ds2);
        ds1.set(0, 0.0, 1.0, 1.0, 0.0);
        assertNotEquals(ds1, ds2);
        ds2.set(0, 0.0, 1.0, 1.0, 0.0);
        assertEquals(ds1, ds2);
        ds1.set(0, 0.0, 1.0, 1.0, 1.0);
        assertNotEquals(ds1, ds2);
        ds2.set(0, 0.0, 1.0, 1.0, 1.0);
        assertEquals(ds1, ds2);
        assertEquals(ds1, ds2);
        ds1.set(0, 0.0, 1.0, 0.0, 0.0);
        assertNotEquals(ds1, ds2);
        ds2.set(0, 0.0, 1.0, 0.0, 0.0);
        assertEquals(ds1, ds2);

        // check near equality of X/Y values
        double delta = 1e-6;
        ds1.set(0, 0.0 + delta, 0.0, 0.0, 0.0);
        assertFalse(ds1.equals(ds2, delta));
        ds2.set(0, 0.0 + delta, 0.0, 0.0, 0.0);
        assertTrue(ds1.equals(ds2, delta));
        ds1.set(0, 0.0, 0.0 + delta, 0.0, 0.0);
        assertFalse(ds1.equals(ds2, delta));
        ds2.set(0, 0.0, 0.0 + delta, 0.0, 0.0);
        assertTrue(ds1.equals(ds2, delta));

        ds1.set(0, 1e9 + 1e9 * delta, 0.0, 0.0, 0.0);
        assertFalse(ds1.equals(ds2, delta));
        ds2.set(0, 1e9 + 3e9 * delta, 0.0, 0.0, 0.0);
        assertEquals(ds1.getAxisDescription(DIM_Y), ds2.getAxisDescription(DIM_Y));
        assertEquals(ds1.getAxisDescription(DIM_X), ds2.getAxisDescription(DIM_X));
        assertTrue(ds1.equals(ds2, delta));

        // check near equality of X/Y error values
        ds1.set(0, 0.0, 0.0, 0.0, 0.0);
        ds2.set(0, 0.0, 0.0, 0.0, 0.0);
        ds1.set(0, 0.0, 0.0, delta, 0.0);
        assertFalse(ds1.equals(ds2, delta));
        ds2.set(0, 0.0, 0.0, delta, 0.0);
        assertTrue(ds1.equals(ds2, delta));

        ds1.set(0, 0.0, 0.0, 0.0, delta);
        assertFalse(ds1.equals(ds2, delta));
        ds2.set(0, 0.0, 0.0, 0.0, delta);
        assertTrue(ds1.equals(ds2, delta));

        ds1.set(0, 0.0, 0.0, 0.0, 0.0);
        ds2.set(0, 0.0, 0.0, 0.0, 0.0);
        ds2.set(0, 0.0, 0.0, 0.0, delta);
        assertFalse(ds1.equals(ds2, delta));
        ds1.set(0, 0.0, 0.0, 0.0, delta);
        assertTrue(ds1.equals(ds2, delta));

        // check dataLabel
        ds1.addDataLabel(0, "Test");
        assertNotEquals(ds1, ds2);
        ds2.addDataLabel(0, "Test");
        assertEquals(ds1, ds2);

        ds1.addDataLabel(0, "ModifiedTest");
        assertNotEquals(ds1, ds2);
        ds2.addDataLabel(0, "ModifiedTest");
        assertEquals(ds1, ds2);

        EditConstraints editConstraints = new NullEditConstraints();
        ds1.setEditConstraints(editConstraints);
        assertNotEquals(ds1, ds2);
        ds2.setEditConstraints(new NullEditConstraints());
        assertNotEquals(ds1, ds2);
        ds2.setEditConstraints(editConstraints);
        assertEquals(ds1, ds2);

        // check meta data
        ds1.getInfoList().add("info");
        assertNotEquals(ds1, ds2);
        ds2.getInfoList().add("info");
        assertEquals(ds1, ds2);
        ds1.getWarningList().add("warning");
        assertNotEquals(ds1, ds2);
        ds2.getWarningList().add("warning");
        assertEquals(ds1, ds2);
        ds1.getErrorList().add("error");
        assertNotEquals(ds1, ds2);
        ds2.getErrorList().add("error");
        assertEquals(ds1, ds2);
        ds1.getMetaInfo().put("testKey", "testValue");
        assertNotEquals(ds1, ds2);
        ds2.getMetaInfo().put("testKey", "testValue");
        assertEquals(ds1, ds2);

        // styles are explicitly not checked
        ds1.addDataStyle(0, "myStyle");
        assertEquals(ds1, ds2);
        ds2.addDataStyle(0, "myStyle");
        assertEquals(ds1, ds2);

        // listeners are explicitly not checked
        ds1.addListener(e -> System.err.print("do nothing"));
        assertEquals(ds1, ds2);
        ds2.addListener(e -> System.err.print("do also nothing"));
        assertEquals(ds1, ds2);
    }

    /**
     * just for testing... N.B. equals test for existing and object equality
     *
     * @author rstein
     */
    private class NullEditConstraints implements EditConstraints {
        @Override
        public boolean canAdd(int index) {
            return false;
        }

        @Override
        public boolean canDelete(int index) {
            return false;
        }

        @Override
        public boolean isEditable(final int dimIndex) {
            return false;
        }
    }

    private class OneDimDataSet extends AbstractDataSet<OneDimDataSet> implements DataSet {
        private static final long serialVersionUID = 1L;
        final private double[] data = new double[] { 2.4, 5.2, 8.5, 9.2 };

        public OneDimDataSet() {
            super("test", 1);
        }

        @Override
        public double get(int dimIndex, int index) {
            if (dimIndex != 0) {
                throw new IndexOutOfBoundsException("Dimension index out of bound");
            }
            return data[index];
        }

        @Override
        public int getDataCount() {
            return data.length;
        }

        @Override
        public int getIndex(int dimIndex, double... value) {
            AssertUtils.checkArrayDimension("value", value, 1);
            if (dimIndex != 0) {
                throw new IndexOutOfBoundsException("Dimension index out of bound");
            }
            return Math.abs(Arrays.binarySearch(data, value[0]));
        }
    }
}
