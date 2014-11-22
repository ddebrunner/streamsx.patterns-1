/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.test.operator;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.patterns.operator.Filter;

/**
 * TestFilter that filters tuples according to the attribute int32 a being
 * greater than or equal to a threshold.
 * 
 */
public class FilterTestOp extends Filter {
    private int threshold;

    @Override
    protected boolean filter(Tuple tuple) throws Exception {
        return tuple.getInt("a") >= getThreshold();
    }

    public int getThreshold() {
        return threshold;
    }

    @Parameter(optional=true)
    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
}
