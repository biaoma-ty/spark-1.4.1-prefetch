package org.apache.spark.network.shuffle;

import java.lang.Throwable; /**
 * Created by INFI on 2015/9/15.
 */
public interface BlockPreparingListener {
    void onBlockPrepareSuccess();
    void onBlockPrepareFailure(Throwable exception);
}
