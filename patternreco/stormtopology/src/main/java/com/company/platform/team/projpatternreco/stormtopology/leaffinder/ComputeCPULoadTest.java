package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import edu.emory.mathcs.backport.java.util.Collections;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;

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
    private static final String projectName = "nelo2-monitoring-alpha";
    private static final double leafSimilarity = 0.9;
    private static final Logger logger =Logger.getLogger(ComputeCPULoadTest.class);
    private static int maxCount = 10;

    public static void main(String[] args) {
        try {
            logger.info("test started....");
            parseArgs(args);
            logs = readLogsFromFile("./logs.txt");
            preparePatternTree(logs, PatternLeaves.getInstance());

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
                createAndStartWorker(barrier, endTime, PatternLeaves.getInstance()); //use barrier to start all workers at the same time as main thread
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

    private static void preparePatternTree(List<String> logs, PatternLeaves leaves) {
        for (String log : logs) {
            List<String> tokens = Preprocessor.transform(log);
            leaves.addNewLeaf(projectName, tokens);
        }
    }

    private static void doFastClustering(List<String> logs, PatternLeaves leaves) {
        for (String log : logs) {
            List<String> tokens = Preprocessor.transform(log);
            //System.out.println(Arrays.toString(tokens.toArray()));
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            PatternNodeKey nodeKey = leaves.getParentNodeId(tokens, levelKey, 1 - leafSimilarity,
                    Constants.FINDCLUSTER_TOLERANCE_TIMES);
            //System.out.println(nodeKey.toString());
        }
    }

    private static void doFastClusteringWithTokens(List<List<String>> logTokens, PatternLeaves nodes) {
        for (List<String> tokens : logTokens) {
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            PatternNodeKey nodeKey = nodes.getParentNodeId(tokens, levelKey, 1 - leafSimilarity,
                    Constants.FINDCLUSTER_TOLERANCE_TIMES);
            //System.out.println(nodeKey.toString());
        }
    }

    private static void createAndStartWorker(CyclicBarrier cyclicBarrier, long endTime, PatternLeaves leaves) {
        new Thread(() -> {
            try {
                cyclicBarrier.await();
                int i=0;
                while (System.currentTimeMillis() < endTime) {
                    // Thread 100% time as RUNNABLE, taking 1/(n cores) of JVM/System overall CPU
                    doFastClustering(logs, leaves);
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

        //CommandLineParser parser = new BasicParser();
        CommandLineParser parser = new GnuParser();
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