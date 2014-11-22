/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.operator;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;

/**
 * Pattern that splits the stream into multiple output streams.
 * <P>
 * The schema of all output ports must exactly
 * match the schema of the input port.
 *
 */

@InputPorts(@InputPortSet(cardinality=1,id="input", description="Tuples to be split across multiple output ports."))
@OutputPorts({
	@OutputPortSet(cardinality=1,description="Output ports the input is split into.",
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving,
			windowPunctuationInputPort="input"),
	@OutputPortSet(optional=true,description="Additional output ports the input is split into.")
})
public abstract class Split extends AbstractOperator {
	
	private int outputPortCount;
	
	/**
	 * {@inheritDoc}
	 * <P>
	 * Obtains the number of output ports for the mod
	 * operation performed against the return of {@link #destination(Tuple)}.
	 * </P>
	 */
	@Override
	public void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		outputPortCount = context.getNumberOfStreamingOutputs();
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
		
		final int destination = destination(tuple);
		if (destination >= 0)
			getOutput(destination % outputPortCount).submit(tuple);
	}
	
	/**
	 * Determine the destination index for the tuple.
	 * <BR>
	 * If the index returned is less than zero then
	 * {@code tuple} is discarded.
	 * <BR>
	 * Otherwise the returned index determines which output
	 * port the tuple is submitted to. The index is modded ({@code %})
	 * by the number of output ports to return the
	 * index (zero-based) of the output port the
	 * tuple is submitted to.
	 * 
	 * @param tuple Tuple to be filtered.
	 * @return Destination index for {@code tuple}.
	 * @throws Exception Exception determining the tuple's destination.
	 */
	protected abstract int destination(Tuple tuple) throws Exception;
	
	/**
	 * Check that the schemas for all output
	 * ports match the first input port, as the tuples are directly
	 * submitted from the input to the output.
	 * @param checker Context checker object.
	 */
	@ContextCheck
	public static void checkMatchingSchemas(OperatorContextChecker checker) {
		
		OperatorContext context = checker.getOperatorContext();
		
		StreamingData inputPort = context.getStreamingInputs().get(0);
		
		for (StreamingData outputPort : context.getStreamingOutputs())
		    checker.checkMatchingSchemas(inputPort, outputPort);	
	}
}
