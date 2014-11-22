package com.ibm.streamsx.patterns.test.operator;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.patterns.operator.RegexFilter;

public class RegexFilterTestOp extends RegexFilter {
    private String pattern;

    public RegexFilterTestOp() {
    }
    
    @Parameter(optional=true)
    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    protected String getExpression() {
        return getPattern();
    }

    @Override
    protected CharSequence getTupleSequence(Tuple tuple) throws Exception {
        return tuple.getString("a");
    }
}