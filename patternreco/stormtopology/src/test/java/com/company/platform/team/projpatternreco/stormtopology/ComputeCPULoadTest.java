package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * Created by admin on 2018/7/16.
 */
//https://bmwieczorek.wordpress.com/2015/11/02/java-monitoring-cpu-and-system-load-of-multi-threaded-application-via-operatingsystemmxbean/
//
public class ComputeCPULoadTest {
    private static List<String> logs;
    private static List<List<String>> logTokens;
    private static final String projectName = "monitoring";
    private static final double leafSimilarity = 0.9;
    private static final Logger logger =Logger.getLogger(ComputeCPULoadTest.class);
    private static int maxCount = 10;
    private static Recognizer nodesUtilInstance = Recognizer.getInstance(prepareConfigure());

    public static void main(String[] args) {
        try {
            logger.info("test started....");
            parseArgs(args);
            logs = readLogsFromFile("./logs.txt");
            preparePatternTree(logs);

            logTokens = new ArrayList<>();
            for (String log : logs) {
                logTokens.add(Preprocessor.transform(log));
            }
            Collections.shuffle(logs, new Random(1234));

            OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
            //lscpu: CPUS = sockets * cores per sockets * thread per core
            int workersCount = Runtime.getRuntime().availableProcessors();
            CyclicBarrier barrier
                    = new CyclicBarrier(workersCount + 1); // + 1 to include main thread measuring CPU load

            long endTime = System.currentTimeMillis() + 5 * 1000;
            for (int i = 0; i < workersCount; i++) {
                createAndStartWorker(barrier, endTime); //use barrier to start all workers at the same time as main thread
            }
            barrier.await();
            System.out.println("All workers and main thread started");
            while (System.currentTimeMillis() <= endTime) {
                getAndPrintCpuLoad(operatingSystemMXBean);
                //TimeUnit.MILLISECONDS.sleep(100);
                TimeUnit.SECONDS.sleep(1);
            }
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
            nodesUtilInstance.addNode(new PatternLevelKey(projectName, 0),
                    new PatternNode(tokens));
        }
    }

    private static void doFastClustering(List<String> logs) throws Exception{
        for (String log : logs) {
            List<String> tokens = Preprocessor.transform(log);
            //System.out.println(Arrays.toString(tokens.toArray()));
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            PatternNodeKey nodeKey = nodesUtilInstance.getParentNodeId(tokens, levelKey, 1 - leafSimilarity,
                    Constants.FINDCLUSTER_TOLERANCE_TIMES);
            //System.out.println(nodeKey.toString());
        }
    }

    private static void doFastClusteringWithTokens(List<List<String>> logTokens) throws Exception{
        for (List<String> tokens : logTokens) {
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            PatternNodeKey nodeKey = nodesUtilInstance.getParentNodeId(tokens, levelKey, 1 - leafSimilarity,
                    Constants.FINDCLUSTER_TOLERANCE_TIMES);
            //System.out.println(nodeKey.toString());
        }
    }

    private static void createAndStartWorker(CyclicBarrier cyclicBarrier, long endTime) {
        new Thread(() -> {
            try {
                cyclicBarrier.await();
                int i=0;
                while (System.currentTimeMillis() < endTime) {
                    // Thread 100% time as RUNNABLE, taking 1/(n cores) of JVM/System overall CPU
                    doFastClustering(logs);
                    //doFastClusteringWithTokens(logTokens);
                    i++;
                }
                logger.info(Thread.currentThread().getName() + " finished, " + i*logs.size() + " logs processed.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void getAndPrintCpuLoad(OperatingSystemMXBean mxBean) {
        // need to use reflection as the impl class is not visible
        for (Method method : mxBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            String methodName = method.getName();
            if (methodName.startsWith("get") && methodName.contains("Cpu") && methodName.contains("Load")
                    && Modifier.isPublic(method.getModifiers())) {
                Object value;
                try {
                    value = method.invoke(mxBean);
                } catch (Exception e) {
                    value = e;
                }
                logger.info(methodName + " = " + value);
            }
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

    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("n", "num", true, "log count");

        CommandLineParser parser = new DefaultParser();
        CommandLine commands = parser.parse(options, args);
        if (commands.hasOption("h")) {
            showHelp(options);
        }

        if (commands.hasOption("n")) {
            maxCount = Integer.parseInt(commands.getOptionValue("n"));
        }
    }

    private static void showHelp(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("PatternRefinerTopology", options);
        System.exit(0);
    }
}