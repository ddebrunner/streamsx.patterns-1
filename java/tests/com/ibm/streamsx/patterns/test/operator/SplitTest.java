/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.patterns.test.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
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
import com.ibm.streamsx.patterns.operator.Split;

public class SplitTest {

    private static final StreamSchema testSchema = Type.Factory.getTupleType(
            "tuple<int32 a, ustring b>").getTupleSchema();

    private final JavaOperatorTester jot = new JavaOperatorTester();

    /**
     * Test the Split with with various numbers of outputs.
     */
    @Test
    public void testSingleOutput() throws Exception {

        Random rand = new Random();

        for (int t = 0; t < 10; t++) {
            OperatorInvocation<SplitTestOp> tf = jot
                    .singleOp(SplitTestOp.class);
            InputPortDeclaration input = tf.addInput(testSchema);
            OutputPortDeclaration[] outputs = new OutputPortDeclaration[rand
                    .nextInt(10) + 1];
            for (int p = 0; p < outputs.length; p++) {
                outputs[p] = tf.addOutput(testSchema);
            }
            tf.graph().compileChecks();
            JavaTestableGraph tester = jot.tester(tf);
            List<MostRecent<Tuple>> lastTuples = new ArrayList<MostRecent<Tuple>>(
                    outputs.length);
            for (OutputPortDeclaration output : outputs) {
                MostRecent<Tuple> lastPassTuple = new MostRecent<Tuple>();
                lastTuples.add(lastPassTuple);
                tester.registerStreamHandler(output, lastPassTuple);
            }

            StreamingOutput<OutputTuple> inject = tester.getInputTester(input);
            tester.initialize().get().allPortsReady().get();
            for (int i = 0; i < 100; i++) {
                for (MostRecent<Tuple> lastTuple : lastTuples)
                    lastTuple.clear();
                int value = rand.nextInt(100);
                inject.submitAsTuple(value, "v" + value);

                if ((value % 7) == 0) {
                    // not passed through.
                    for (MostRecent<Tuple> lastTuple : lastTuples)
                        assertNull(lastTuple.getMostRecentTuple());

                } else {
                    int port = (value + 37) % outputs.length;
                    MostRecent<Tuple> portLastTuple = lastTuples.get(port);
                    assertNotNull(portLastTuple.getMostRecentTuple());
                    assertEquals(value, portLastTuple.getMostRecentTuple()
                            .getInt("a"));
                    assertEquals("v" + value, portLastTuple
                            .getMostRecentTuple().getString("b"));

                    // Ensure only seen on a single port.
                    for (MostRecent<Tuple> lastTuple : lastTuples)
                        if (lastTuple != portLastTuple)
                            assertNull(lastTuple.getMostRecentTuple());
                }
            }
            tester.shutdown().get();
        }
    }

    @Test
    public void testNonMatchingPorts() throws Exception {
        // testNonMatchingPorts(jot, TestFilter.class);

        OperatorInvocation<? extends Split> tf = jot
                .singleOp(SplitTestOp.class);
        tf.addInput(testSchema);
        tf.addOutput(testSchema.extend("float64", "c"));
        assertFalse(tf.graph().compileChecks());

        // Non-match pass port with second port
        tf = jot.singleOp(SplitTestOp.class);
        tf.addInput(testSchema);
        tf.addOutput(testSchema.extend("float64", "c"));
        tf.addOutput(testSchema);
        assertFalse(tf.graph().compileChecks());

        // Non-match non-pass port
        tf = jot.singleOp(SplitTestOp.class);
        tf.addInput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema.extend("float64", "c"));
        assertFalse(tf.graph().compileChecks());

        // No matching ports
        tf = jot.singleOp(SplitTestOp.class);
        tf.addInput(testSchema);
        tf.addOutput(testSchema.extend("float64", "d"));
        tf.addOutput(testSchema.extend("float64", "c"));
        assertFalse(tf.graph().compileChecks());

        tf = jot.singleOp(SplitTestOp.class);
        tf.addInput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema);
        tf.addOutput(testSchema.extend("float64", "c"));
        assertFalse(tf.graph().compileChecks());

    }
}
