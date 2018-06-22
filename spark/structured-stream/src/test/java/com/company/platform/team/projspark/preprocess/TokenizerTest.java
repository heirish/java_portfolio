package com.company.platform.team.projspark.preprocess;

import org.apache.log4j.Logger;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2018/6/21.
 */
public class TokenizerTest {
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
    public void simpleTokenizerTestSpace() {
        // will automatically delete the excessive spaces
        String text = "wpbfl2-45.gate.net# [29:23:57:53] aeadf ewfasd ] !\"w   treespaces";
        List<String> tokens = Tokenizer.simpleTokenize(text);
        logger.info(tokens.toString());
        logger.info(text);
        logger.info(String.join("", tokens));
    }

    @Test
    public void simpleTokenizerTest() {
        long start = System.currentTimeMillis();
        String text = "wpbfl2-45.gate.net#123$adsf%123&jj'ad(ad)ed+344d,wsdf [29:23:57:53]"
                + " !\"wefsadf'eafedf<=>ea;sdf?esdf@234[erfasdf]sdf\\a^`b{|}c~"
                + " \"GET /cgi-bin/waisgate?port=210&ip_address=earth1.epa.gov&database_name="
                + "/indexes/ACCESS&headline=Safe%20Drinking%20Water%20Hotline&type=HTML"
                + "&docid=%01%0aearth1%3a210%02%0f%2findexes%2fACCESS%03%3b0%20%2d2262%20%2fusr1%2fearth1%2fjstaffor"
                + "%2faccessepa%2fchapter3%2fs3%2d18%2ehtml%04%0aearth1%3a210%05%0f%2findexes"
                + "%2fACCESS%06%3b0%20%2d2262%20%2fusr1%2fearth1%2fjstaffor%2faccessepa%2fchapter3"
                + "%2fs3%2d18%2ehtml%07%01%00&seed_words_used=drinking+water&byte_count=2262 HTTP/1.0\\\" 200 2431";
        List<String> tokens = Tokenizer.simpleTokenize(text);
        long end = System.currentTimeMillis();
        logger.info("info: simpleTokenizerTest took " + (end - start) + " MilliSeconds");
        logger.info(tokens.toString());
        logger.info(text);
        logger.info(String.join("", tokens));
    }

    @Test
    public void simpleTokenizerTestNoPunct() {
        String text = "abc";
        logger.info(Character.UnicodeScript.HAN.toString());
        List<String> tokens = Tokenizer.simpleTokenize(text);
        logger.info(tokens.toString());
    }

    @Test
    public void simpleTokenizerTestCNPunct(){
        String text = "与韩国NAVER网官方博客的应用程序，您可以检查您的博客和实时状态的最新更新你的邻居。"
                + "此外，您可以轻松地发布职位或位置信息的照片。韩国NAVER网博客仅仅是正确的方式分享您的故事与世界！";
        // logger.info(Character.UnicodeScript.HAN.toString());
        List<String> tokens = Tokenizer.simpleTokenize(text);
        logger.info(tokens.toString());
    }
}
