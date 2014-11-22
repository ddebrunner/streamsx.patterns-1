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
 * Filter based upon a regular expression using
 * {@code java.util.regex.Pattern and Matcher}.
 *
 */
public abstract class RegexFilter extends Filter {
	
	private Matcher matcher;
	
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		
		matcher = createPattern().matcher("");
	}
	
	protected Pattern createPattern() {
		return Pattern.compile(getExpression());
	}

	/**
	 * Match against the character sequence returned by
	 * {@link #getTupleSequence(Tuple)} using the
	 * pattern returned by {@link #getExpression()}.
	 * <BR>
	 * Method is {@code synchronized} as a {@code Matcher} is
	 * not thread safe.
	 * 
	 * @return {@code true} if the pattern matches the expression,
	 * {@code false} otherwise.
	 */
	@Override
	protected synchronized boolean filter(Tuple tuple) throws Exception {
		matcher.reset(getTupleSequence(tuple));
		return matcher.matches();
	}
	
	/**
	 * Regular expression to be used to match input tuples.
	 * This is called once, during {@link #initialize(OperatorContext)}.
	 * @return Regular expression for pattern matching.
	 */
	protected abstract String getExpression();
	
	/**
	 * Return the character sequence from {@tuple} to be
	 * matched against the regular expression.
	 * @param tuple INput tuple.
	 * @return The character sequence from {@tuple} to be
	 * matched against the regular expression.
	 * @throws Exception Exception determ
	 */
	protected abstract CharSequence getTupleSequence(Tuple tuple) throws Exception;
}
