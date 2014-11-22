/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.test.operator;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.patterns.operator.Split;

/**
 * TestFilter that filters tuples according to the attribute int32 a being
 * greater than or equal to a threshold.
 * 
 */
public class SplitTestOp extends Split {

    @Override
    protected int destination(Tuple tuple) throws Exception {
        int a = tuple.getInt("a");
        if ((a % 7) == 0)
            return -1;

        return a + 37;
    }
}
