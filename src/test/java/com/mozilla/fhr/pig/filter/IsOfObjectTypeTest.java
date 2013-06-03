package com.mozilla.fhr.pig.filter;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;



public class IsOfObjectTypeTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
    @Test
    public void testExec1() throws IOException {
        IsOfObjectType filter = new IsOfObjectType(null);
        assertFalse(filter.exec(null));
    }

    @Test
    public void testExec2() throws IOException {
        IsOfObjectType isIntegerObjectFilter = new IsOfObjectType("Integer");
        IsOfObjectType isStringObjectFilter = new IsOfObjectType("String");

        Tuple integerInputTuple = tupleFactory.newTuple(0);
        integerInputTuple.append(new Integer(0));

        Tuple longInputTuple = tupleFactory.newTuple(0);
        longInputTuple.append(new Long(0));

        Tuple stringInputTuple = tupleFactory.newTuple(0);
        stringInputTuple.append("abc");

        assertTrue(isIntegerObjectFilter.exec(integerInputTuple));
        assertTrue(isIntegerObjectFilter.exec(longInputTuple));
        assertFalse(isIntegerObjectFilter.exec(stringInputTuple));

        assertFalse(isStringObjectFilter.exec(integerInputTuple));
        assertFalse(isStringObjectFilter.exec(longInputTuple));
        assertTrue(isStringObjectFilter.exec(stringInputTuple));
    }

}
