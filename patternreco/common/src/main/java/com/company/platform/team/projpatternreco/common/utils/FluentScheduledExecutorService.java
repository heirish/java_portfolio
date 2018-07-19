package com.company.platform.team.projpatternreco.common.utils;

import org.apache.log4j.Logger;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2018/7/1 0001.
 */
public class FluentScheduledExecutorService extends ScheduledThreadPoolExecutor {
    private static final Logger logger = Logger.getLogger("");

    public FluentScheduledExecutorService(int nThreads) {
        super(nThreads);
    }

    @Override
    public ScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay,
                                               long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(wrapRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                  long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(wrapRunnable(command), initialDelay, delay, unit);
    }

    private Runnable wrapRunnable(Runnable command) {
        return new TaskRunnable(command);
    }

    private class TaskRunnable implements Runnable {
        private Runnable theRunnable;

        public TaskRunnable(Runnable theRunnable) {
            super();
            this.theRunnable = theRunnable;
        }

        @Override
        public void run() {
            try {
                theRunnable.run();
            } catch (Exception e) {
                logger.error("error in executing: " + theRunnable + ". It will no longer be run!", e);
                // and re throw it so that the Executor also gets this error so that it can do what it would
                // usually do
                // if silence, do not throw, if not silence, throw this exception
                // throw new RuntimeException(e);
            }
        }
    }
}
