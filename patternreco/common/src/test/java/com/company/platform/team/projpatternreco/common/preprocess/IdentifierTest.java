package com.company.platform.team.projpatternreco.common.preprocess;

import org.apache.log4j.Logger;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * Created by admin on 2018/6/20.
 */
public class IdentifierTest {
    private static final Logger logger = Logger.getLogger("");

    private static void logInfo(Description description, String status, long nanos) {
        String testName = description.getMethodName();
        logger.info(String.format("Test %s %s, spent %d microseconds",
                testName, status, TimeUnit.NANOSECONDS.toMicros(nanos)));
    }

    @Rule
    public Stopwatch stopwatch = new Stopwatch() {
        @Override
        protected void succeeded(long nanos, Description description) {
            logInfo(description, "succeeded", nanos);
        }

        @Override
        protected void failed(long nanos, Throwable e, Description description) {
            logInfo(description, "failed", nanos);
        }

        @Override
        protected void skipped(long nanos, AssumptionViolatedException e, Description description) {
            logInfo(description, "skipped", nanos);
        }

        @Override
        protected void finished(long nanos, Description description) {
            logInfo(description, "finished", nanos);
        }
    };

    @Test
    public void identifyIPWithoutIP(){
        String text = "this is a test string without ip";
        logger.info(Identifier.identifyIP(text, "NELO_IP"));
        assertEquals(Identifier.identifyIP(text, "NELO_IP"), text);
    }

    @Test
    public void identifyIPWithInvalidIP(){
        String text = "this is a test string with 192.168.1";
        logger.info(Identifier.identifyIP(text, "NELO_IP"));
        assertEquals(Identifier.identifyIP(text, "NELO_IP"), text);
    }

    @Test
    public void identifyIPWithIPV4(){
        String text = "this is a test string with ip 192.168.1.1";
        logger.info(Identifier.identifyIP(text, "NELO_IP"));
        assertEquals(Identifier.identifyIP(text, "NELO_IP"), "this is a test string with ip NELO_IP");
    }

    @Test
    public void identifyIPWithIPV6(){
        String text = "this is a test string with ip 3ffe:1900:4545:3:200:f8ff:fe21:67cf";
        logger.info(Identifier.identifyIP(text, "NELO_IP"));
        assertEquals(Identifier.identifyIP(text, "NELO_IP"), "this is a test string with ip NELO_IP");
    }

    @Test
    public void identifyIPWithIPV4AndV6(){
        String text = "this is a test string with ip 192.168.1.1 and 3ffe:1900:4545:3:200:f8ff:fe21:67cf";
        logger.info(Identifier.identifyIP(text, "NELO_IP"));
        assertEquals(Identifier.identifyIP(text, "NELO_IP"),
                "this is a test string with ip NELO_IP and NELO_IP");
    }
}
