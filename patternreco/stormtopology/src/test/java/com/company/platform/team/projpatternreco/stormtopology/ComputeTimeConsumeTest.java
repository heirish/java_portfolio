package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Created by admin on 2018/7/16.
 */
//https://bmwieczorek.wordpress.com/2015/11/02/java-monitoring-cpu-and-system-load-of-multi-threaded-application-via-operatingsystemmxbean/
//
public class ComputeTimeConsumeTest {
    private static List<String> logs;
    private static final String projectName = "monitoring";
    private static final double leafSimilarity = 0.9;
    private static final Logger logger =Logger.getLogger(ComputeTimeConsumeTest.class);
    private static int maxCount = -1;
    private static Recognizer recognizer = Recognizer.getInstance(prepareConfigure());

    public static void main(String[] args) {
        try {
            logger.info("test started....");
            logs = readLogsFromFile("./logs.txt");
            System.out.println(Arrays.toString(logs.toArray()));
            preparePatternTree(logs);
            Collections.shuffle(logs, new Random(1234));
            System.out.println(Arrays.toString(logs.toArray()));

            doFastClustering(logs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, String> prepareConfigure() {
        Map<String, String> conf = new HashMap<>();
        conf.put("host", "");
        conf.put("port", "");
        conf.put("maxTotal", "2000");
        conf.put("maxWaitMillis", "5000");
        return conf;
    }

    private static void preparePatternTree(List<String> logs) {
        for (String log : logs) {
            List<String> tokens = Preprocessor.transform(log);
            recognizer.addNode(new PatternLevelKey(projectName, 0),
                    new PatternNode(tokens));
        }
    }

    private static void doFastClustering(List<String> logs) throws Exception{
        long preprocessTime = 0;
        long finderTime = 0;
        for (int i=0; i< 100; i++) {
            for (String log : logs) {
                long startTime = System.currentTimeMillis();
                List<String> tokens = Preprocessor.transform(log);
                long endTime = System.currentTimeMillis();
                preprocessTime += endTime - startTime;
                //System.out.println(Arrays.toString(tokens.toArray()));
                PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
                startTime = System.currentTimeMillis();
                //PatternNodeKey nodeKey = PatternNodes.getInstance().getParentNodeId(tokens, levelKey, 1 - leafSimilarity);
                recognizer.getLeafNodeId(projectName, log);
                endTime = System.currentTimeMillis();
                finderTime += endTime - startTime;
                //System.out.println(nodeKey.toString());
            }
        }
        System.out.println("preprocessTime: " + preprocessTime);
        System.out.println("finderTime: " + finderTime);
    }

    private static void doFastClusteringWithTokens(List<List<String>> logTokens) throws Exception{
        for (List<String> tokens : logTokens) {
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            PatternNodeKey nodeKey = recognizer.getParentNodeId(levelKey, tokens);
            //System.out.println(nodeKey.toString());
        }
    }

    private static List<String> readLogsFromFile(String fileName) {
        System.out.println(maxCount);
        List<String> logs = new ArrayList<String>();
        int i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                logs.add(line);
                i++;
                if (maxCount > 1 && i > maxCount) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return logs;
    }
}