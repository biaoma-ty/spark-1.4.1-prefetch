package org.apache.spark.network.shuffle;

import org.apache.spark.network.client.PrepareRequestRecievedCallBack;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.PrepareBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;import java.lang.IllegalArgumentException;import java.lang.Override;import java.lang.String;import java.lang.Throwable;

/**
 * Created by INFI on 2015/9/15.
 */
public class BlockToPrepareInfoSender {
    private final Logger logger = LoggerFactory.getLogger(BlockToPrepareInfoSender.class);

    private final TransportClient client;
    private final PrepareBlocks prepareMessage;
    private final String[] blockIds;
    private final String[] blocksToRelease;
    private final BlockPreparingListener listener;
    private final PrepareRequestRecievedCallBack requestRecievedCallBack;

    public BlockToPrepareInfoSender(TransportClient client,
                                    String appId,
                                    String execId,
                                    String[] blockIds,
                                    String[] blocksToRelease,
                                    BlockPreparingListener listener) {
        this.client = client;
        this.prepareMessage = new PrepareBlocks(appId, execId, blockIds, blocksToRelease);
        this.blockIds = blockIds;
        this.blocksToRelease = blocksToRelease;
        this.listener = listener;
        this.requestRecievedCallBack = new PrepareCallBack();

        logger.info("BM@PrepareInfoSener blocksToRelease size " + blocksToRelease.length);
        logger.debug("BM@BlockPreparer sender constructor called");
    }

    private class PrepareCallBack implements  PrepareRequestRecievedCallBack {
        @Override
        public void onSuccess() {
            listener.onBlockPrepareSuccess();
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onBlockPrepareFailure(e);
        }
    }

    public void start() {
        if (blockIds.length == 0) {
            throw new IllegalArgumentException("Zero-size blockIds array");
        }

        logger.debug("PrepareMessageSender start method called");
        client.sendRpc(prepareMessage.toByteArray(), new RpcResponseCallback() {
            @Override
            public void onSuccess(byte[] response) {
                logger.debug("Successfully send prepare block's info, ready for the next step");
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Failed while send the prepare message");
            }
        });
    }
}