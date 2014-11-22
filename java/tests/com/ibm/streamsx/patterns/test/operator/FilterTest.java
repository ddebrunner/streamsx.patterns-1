/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.test.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Random;

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
import com.ibm.streamsx.patterns.operator.Filter;

public class FilterTest {
		
	private static final StreamSchema testSchema =
			Type.Factory.getTupleType("tuple<int32 a, ustring b>").getTupleSchema();
	
	private final JavaOperatorTester jot = new JavaOperatorTester();
	
	/**
	 * Test the filter with a single output.
	 */
	@Test
	public void testSingleOutput() throws Exception {
		
		Random rand = new Random();
		
		for (int t = 0; t < 10; t++) {
			final int threshold = rand.nextInt(100);
			OperatorInvocation<TestFilter> tf = jot.singleOp(TestFilter.class);
			tf.setIntParameter("threshold", threshold);
			InputPortDeclaration input = tf.addInput(testSchema);
			OutputPortDeclaration pass = tf.addOutput(testSchema);
			tf.graph().compileChecks();
			JavaTestableGraph tester = jot.tester(tf);
			MostRecent<Tuple> lastPassTuple = new MostRecent<Tuple>();
			tester.registerStreamHandler(pass, lastPassTuple);
			StreamingOutput<OutputTuple> inject = tester.getInputTester(input);
			tester.initialize().get().allPortsReady().get();
			for (int i = 0; i < 100; i++) {
				lastPassTuple.clear();
				int value = rand.nextInt(100);
				inject.submitAsTuple(value, "v" + value);
				if (value >= threshold) {
					assertNotNull(lastPassTuple.getMostRecentTuple());
					assertEquals(value,
							lastPassTuple.getMostRecentTuple().getInt("a"));
					assertEquals("v" + value, lastPassTuple.getMostRecentTuple()
							.getString("b"));
				} else {
					assertNull(lastPassTuple.getMostRecentTuple());
				}
			}
			tester.shutdown().get();
		}
	}
	
	/**
	 * Test the filter with a two outputs.
	 */
	@Test
	public void testDoubleOutput() throws Exception {
		
		Random rand = new Random();
				
		for (int t = 0; t < 10; t++) {
			final int threshold = rand.nextInt(100);
			OperatorInvocation<TestFilter> tf = jot.singleOp(TestFilter.class);
			tf.setIntParameter("threshold", threshold);
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
			for (int i = 0; i < 100; i++) {
				lastPassTuple.clear();
				lastNotPassTuple.clear();
				
				int value = rand.nextInt(100);
				inject.submitAsTuple(value, "v" + value);
				if (value >= threshold) {
					assertNotNull(lastPassTuple.getMostRecentTuple());
					assertEquals(value,
							lastPassTuple.getMostRecentTuple().getInt("a"));
					assertEquals("v" + value, lastPassTuple.getMostRecentTuple()
							.getString("b"));
					
					assertNull(lastNotPassTuple.getMostRecentTuple());
				} else {
					assertNotNull(lastNotPassTuple.getMostRecentTuple());
					assertEquals(value,
							lastNotPassTuple.getMostRecentTuple().getInt("a"));
					assertEquals("v" + value, lastNotPassTuple.getMostRecentTuple()
							.getString("b"));

					
					assertNull(lastPassTuple.getMostRecentTuple());
				}
			}
			tester.shutdown().get();
		}
	}
	
	@Test
	public void testNonMatchingPassPort() throws Exception {
		testNonMatchingPorts(jot, TestFilter.class);	
	}
	
	public static void testNonMatchingPorts(JavaOperatorTester jot,
			Class<? extends Filter> filterClass) throws Exception {

		OperatorInvocation<? extends Filter> tf = jot.singleOp(filterClass);
		tf.addInput(testSchema);
		tf.addOutput(testSchema.extend("float64", "c"));
		assertFalse(tf.graph().compileChecks());

		// Non-match pass port with second port
		tf = jot.singleOp(filterClass);
		tf.addInput(testSchema);
		tf.addOutput(testSchema.extend("float64", "c"));
		tf.addOutput(testSchema);
		assertFalse(tf.graph().compileChecks());

		// Non-match non-pass port
		tf = jot.singleOp(filterClass);
		tf.addInput(testSchema);
		tf.addOutput(testSchema);
		tf.addOutput(testSchema.extend("float64", "c"));
		assertFalse(tf.graph().compileChecks());

		// No matching ports
		tf = jot.singleOp(filterClass);
		tf.addInput(testSchema);
		tf.addOutput(testSchema.extend("float64", "d"));
		tf.addOutput(testSchema.extend("float64", "c"));
		assertFalse(tf.graph().compileChecks());
	}
	
	
	/**
	 * TestFilter that filters tuples according to
	 * the attribute int32 a being greater than or equal
	 * to a threshold.
	 *
	 */
	public static class TestFilter extends Filter {
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

}
