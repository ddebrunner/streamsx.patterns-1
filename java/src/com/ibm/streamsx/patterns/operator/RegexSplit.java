/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.operator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;

/**
 * Split the input stream based upon regular expressions using
 * {@code java.util.regex.Pattern} and {@code Matcher}. When a tuple arrives at
 * the input port it is evaluated against each expression in order, using the
 * value returned by {{@link #getTupleSequence(Tuple)}. The first expression
 * that has a match determines which output port the tuple is sent to, based upon
 * the index of the expression in the return of {@link #getExpressions()}.
 */
public abstract class RegexSplit extends Split {
	
	private Matcher[] matchers;
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		
		final String[] expressions = getExpressions();
		
		matchers = new Matcher[expressions.length];
		
		for (int i = 0; i < expressions.length; i++) {
			matchers[i] = createPattern(expressions[i]).matcher("");
		}
	}
	
	protected Pattern createPattern(String expression) {
		return Pattern.compile(expression);
	}

	/**
	 * Match against the character sequence returned by
	 * {@link #getTupleSequence(Tuple)} using the
	 * patterns returned by {@link #getExpressions()}.
	 * <BR>
	 * Method is {@code synchronized} as a {@code Matcher} is
	 * not thread safe.
	 * 
	 * @return index of expression if the pattern matches an expression,
	 * {@code -1} otherwise.
	 */
	@Override
	protected synchronized int destination(Tuple tuple) throws Exception {
		final CharSequence sequence = getTupleSequence(tuple);
		final Matcher[] ms = matchers;
		for (int i = 0; i < ms.length; i++) {
			if (ms[i].reset(sequence).matches())
				return i;
		}
		return -1;
	}
	
	/**
	 * Regular expressions to be used to match input tuples.
	 * <BR>
	 * The number of expressions typically matches the number
	 * of output ports, but this is not required. The index
	 * (in the returned array) of the first expression that
	 * matches (starting from 0) is used as the destination index.
	 * <BR>
	 * This is called once, during {@link #initialize(OperatorContext)}.
	 * @return Regular expressions for pattern matching.
	 */
	protected abstract String[] getExpressions();
	
	/**
	 * Return the character sequence from {@tuple} to be
	 * matched against the regular expressions.
	 * @param tuple Input tuple.
	 * @return The character sequence from {@tuple} to be
	 * matched against the regular expressions.
	 * @throws Exception Exception determining the tuple sequence.
	 */
	protected abstract CharSequence getTupleSequence(Tuple tuple) throws Exception;
}
