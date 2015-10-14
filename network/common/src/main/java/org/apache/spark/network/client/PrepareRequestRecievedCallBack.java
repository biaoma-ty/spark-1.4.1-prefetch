package org.apache.spark.network.client;

/**
 * Created by INFI on 2015/9/16.
 */
public interface PrepareRequestRecievedCallBack {
    void onSuccess();

    void onFailure(Throwable e);
}
