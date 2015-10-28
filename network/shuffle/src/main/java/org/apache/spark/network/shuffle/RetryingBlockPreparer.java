package org.apache.spark.network.shuffle;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *  * Created by INFI on 2015/9/15.
 *   */
public class RetryingBlockPreparer {

    public static interface PreparerStarter {
        void createAndStart(String[] prepareBlockIds, String[] releaseBlocks, BlockPreparingListener listener) throws IOException;
    }

    private static final ExecutorService executorService = Executors.newCachedThreadPool(
            NettyUtils.createThreadFactory("Prepare Info Send Retry")
    );

    private final Logger logger = LoggerFactory.getLogger(RetryingBlockPreparer.class);

    private final PreparerStarter preparerStarter;

    private final BlockPreparingListener listener;

    private final int maxRetries;

    private final int retryWaitTime;

    private int retryCount = 0;

    private final LinkedHashSet<String> outstandingBlockInfosForPrepare;

    private final LinkedHashSet<String> outStandingBlockInfosForRelease;

    private RetryingBlockPreparerListener currentListener;

    public RetryingBlockPreparer(
            TransportConf conf,
            PreparerStarter prepareStarter,
            String[] prepareBlockIds,
            String[] releaseBlockIds,
            BlockPreparingListener listener) {
        this.preparerStarter = prepareStarter;
        this.listener = listener;
        this.maxRetries = conf.maxIORetries();
        this.retryWaitTime = conf.ioRetryWaitTimeMs();
        this.outstandingBlockInfosForPrepare = Sets.newLinkedHashSet();
        this.outStandingBlockInfosForRelease = Sets.newLinkedHashSet();
        Collections.addAll(outstandingBlockInfosForPrepare, prepareBlockIds);
        Collections.addAll(outStandingBlockInfosForRelease, releaseBlockIds);
        this.currentListener = new RetryingBlockPreparerListener();
    }

    public void start(){
        senAllOutStanding();
    }

    private void senAllOutStanding() {
        String[] blockIdsToSendForPrepare;
        String[] blockIdsToSendForRelease;
        int numRetries;
        RetryingBlockPreparerListener myListener;
        synchronized (this) {
            blockIdsToSendForPrepare = outstandingBlockInfosForPrepare.toArray(new String[outstandingBlockInfosForPrepare.size()]);
            blockIdsToSendForRelease = outStandingBlockInfosForRelease.toArray(new String[outStandingBlockInfosForRelease.size()]);
            numRetries = retryCount;
            myListener = currentListener;
        }

        try {
            preparerStarter.createAndStart(blockIdsToSendForPrepare, blockIdsToSendForRelease ,myListener);
            listener.onBlockPrepareSuccess();
        } catch (Exception e) {
            logger.error(String.format("Exception while begin send %s outstanding block info %s",
                    blockIdsToSendForPrepare.length, numRetries > 0 ? "(after )" + numRetries + "retries)" : ""), e);
            if (shouldRetry(e)) {
                initiateRetry();
            } else {
                for (String bid: blockIdsToSendForPrepare) {
                    listener.onBlockPrepareFailure(e);
                }
            }
        }
    }

    private synchronized void initiateRetry(){
        retryCount += 1;
        currentListener = new RetryingBlockPreparerListener();
        logger.info("Retrying send ({}/{}) for {} outstading_prepare and release blocks after {} ms",
                retryCount, maxRetries, outstandingBlockInfosForPrepare.size()+outStandingBlockInfosForRelease.size(), retryWaitTime);

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
                senAllOutStanding();
            }
        });
    }

    private synchronized boolean shouldRetry(Throwable e) {
        boolean isIOException = e instanceof  IOException
                || (e.getCause() != null && e.getCause() instanceof  IOException);
        boolean hasRemainRetries = retryCount < maxRetries;
        return  isIOException && hasRemainRetries;
    }

    private class RetryingBlockPreparerListener implements BlockPreparingListener {
        @Override
        public void onBlockPrepareSuccess() {
            boolean shouldForwardSuccess = false;
            synchronized (RetryingBlockPreparer.this) {
                if (this == currentListener) {
                    shouldForwardSuccess = true;
                }
            }

            if (shouldForwardSuccess) {
                listener.onBlockPrepareSuccess();
            }
        }

        @Override
        public void onBlockPrepareFailure(Throwable exception) {
            boolean shouldForwardFailure = false;
            synchronized (RetryingBlockPreparer.this) {
                if (this == currentListener) {
                    initiateRetry();
                } else {
                    logger.error(String.format("BM@PrepareBlock failed to send blocks' info, " +
                            "and will not retry (%s retries)", retryCount), exception);
                    shouldForwardFailure = true;
                }
            }

            if (shouldForwardFailure) {
                listener.onBlockPrepareFailure(exception);
            }
        }
    }
}

