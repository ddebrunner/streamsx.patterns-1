/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.test.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.patterns.operator.RegexFilter;

public class RegexFilterTest {
		
	private static final StreamSchema testSchema =
			Type.Factory.getTupleType("tuple<ustring a>").getTupleSchema();
	
	private final JavaOperatorTester jot = new JavaOperatorTester();
	
	/**
	 * Test the filter with a single output.
	 */
	@Test
	public void testSingleOutput() throws Exception {
		
			OperatorInvocation<TestRegexFilter> tf = jot.singleOp(TestRegexFilter.class);
			tf.setStringParameter("pattern", "tst.*22");
			InputPortDeclaration input = tf.addInput(testSchema);
			OutputPortDeclaration pass = tf.addOutput(testSchema);
			tf.graph().compileChecks();
			JavaTestableGraph tester = jot.tester(tf);
			MostRecent<Tuple> lastPassTuple = new MostRecent<Tuple>();
			tester.registerStreamHandler(pass, lastPassTuple);
			StreamingOutput<OutputTuple> inject = tester.getInputTester(input);
			tester.initialize().get().allPortsReady().get();
			
			
		    lastPassTuple.clear();
		    inject.submitAsTuple("tst9fa922");
			assertNotNull(lastPassTuple.getMostRecentTuple());
		    assertEquals("tst9fa922", lastPassTuple.getMostRecentTuple().getString("a"));
		    
		    lastPassTuple.clear();
		    inject.submitAsTuple("tst9fa92a");
		    assertNull(lastPassTuple.getMostRecentTuple());

			tester.shutdown().get();
	}
	
	/**
	 * Test the filter with a two outputs.
	 */
	@Test
	public void testDoubleOutput() throws Exception {
		
		OperatorInvocation<TestRegexFilter> tf = jot.singleOp(TestRegexFilter.class);
		tf.setStringParameter("pattern", "tst.*22");
		InputPortDeclaration input = tf.addInput(testSchema);
		OutputPortDeclaration pass = tf.addOutput(testSchema);
		OutputPortDeclaration notPass = tf.addOutput(testSchema);
		tf.graph().compileChecks();
		JavaTestableGraph tester = jot.tester(tf);
		MostRecent<Tuple> lastPassTuple = new MostRecent<Tuple>();
		tester.registerStreamHandler(pass, lastPassTuple);
		MostRecent<Tuple> lastNotPassTuple = new MostRecent<Tuple>();
		tester.registerStreamHandler(notPass, lastNotPassTuple);
	
		StreamingOutput<OutputTuple> inject = tester.getInputTester(input);
		tester.initialize().get().allPortsReady().get();
		
		
	    lastPassTuple.clear();
	    lastNotPassTuple.clear();
	    inject.submitAsTuple("tst9fa922");
		assertNotNull(lastPassTuple.getMostRecentTuple());
	    assertEquals("tst9fa922", lastPassTuple.getMostRecentTuple().getString("a"));
	    assertNull(lastNotPassTuple.getMostRecentTuple());
	    
	    lastPassTuple.clear();
	    lastNotPassTuple.clear();
	    inject.submitAsTuple("tst9fa92a");
	    assertNull(lastPassTuple.getMostRecentTuple());
	    assertEquals("tst9fa92a", lastNotPassTuple.getMostRecentTuple().getString("a"));

		tester.shutdown().get();
	}
	
	@Test
	public void testNonMatchingPassPort() throws Exception {
		FilterTest.testNonMatchingPorts(jot, TestRegexFilter.class);	
	}
	
	
	/**
	 * TestFilter that filters tuples according to
	 * the attribute int32 a being greater than or equal
	 * to a threshold.
	 *
	 */
	public static class TestRegexFilter extends RegexFilter {
		private String pattern = "";

		public String getPattern() {
			return pattern;
		}

		@Parameter(optional=true)
		public void setPattern(String pattern) {
			this.pattern = pattern;
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

}
