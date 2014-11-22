/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.operator;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;

/**
 * Pattern that filters tuples.
 * Input tuples that pass the filter are submitted
 * to the first output port. If a second port
 * exists then input tuples that do not pass
 * the filter are submitted to it.
 * <P>
 * The schema of the first two output ports must exactly
 * match the schema of the input port.
 *
 */

@InputPorts(@InputPortSet(cardinality=1,id="input", description="Tuples to be filtered."))
@OutputPorts({
	@OutputPortSet(cardinality=1,description="Tuples that pass the filter.",
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving,
			windowPunctuationInputPort="input"),
	@OutputPortSet(cardinality=1,optional=true,description="Tuples that do not pass the filter.")
})
public abstract class Filter extends AbstractOperator {
	
	private StreamingOutput<?> matchPort;
	private StreamingOutput<?> notMatchPort;
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		matchPort = getOutput(0);
		if (context.getNumberOfStreamingOutputs() >= 2)
			notMatchPort = getOutput(1);
	}
	
	/**
	 * Invokes {@link #filter(Tuple)} on {@code tuple}. If it
	 * returns {@code true} then the tuple is submitted to the
	 * first output port. If it returns {@code false} then
	 * the tuple is submitted to the second output port if it exists,
	 * otherwise it is discarded.
	 */
	@Override
	public void process(StreamingInput<Tuple> stream, final Tuple tuple)
			throws Exception {
		
		if (filter(tuple))
			matchPort.submit(tuple);
		else if (notMatchPort != null)
			notMatchPort.submit(tuple);		
	}
	
	/**
	 * Filter the tuple. Tuples that pass the filter are submitted to
	 * the first output port (index 0), tuples that do not pass the filter
	 * are submitted to the second output port (index 1) if it exists.
	 * @param tuple Tuple to be filtered.
	 * @return {@code true} if the tuple passes the filter, {@code false} otherwise.
	 * @throws Exception Exception filtering the tuple.
	 */
	protected abstract boolean filter(Tuple tuple) throws Exception;
	
	/**
	 * Check that the schemas for the first and second output
	 * ports match the first input port, as the tuples are directly
	 * submitted from the input to the output.
	 * @param checker Context checker object.
	 */
	@ContextCheck
	public static void checkMatchingSchemas(OperatorContextChecker checker) {
		
		OperatorContext context = checker.getOperatorContext();
		
		StreamingData inputPort = context.getStreamingInputs().get(0);
		
		// Matching port must be the same
		checker.checkMatchingSchemas(inputPort,
				context.getStreamingOutputs().get(0));
		
		// as must be the non-match port if it exists
		if (context.getNumberOfStreamingOutputs() >= 2)
			checker.checkMatchingSchemas(inputPort,
					context.getStreamingOutputs().get(1));			
	}
}
