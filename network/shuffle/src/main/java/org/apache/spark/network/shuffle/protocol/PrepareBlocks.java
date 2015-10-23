package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;
import java.util.Arrays;

/**
 * Created by INFI on 2015/9/15.
 */
public class PrepareBlocks extends BlockTransferMessage{
    public final String appId;
    public final String execId;
    public final String[] blockIds;
    public final String[] blockIdsToRelease;

    public PrepareBlocks (String appId, String execId, String[] blockIdsToPrepare, String[] blockIdsToRelease) {
        this.appId = appId;
        this.execId = execId;
        this.blockIds = blockIdsToPrepare;
        this.blockIdsToRelease = blockIdsToRelease;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId) * 41 + Arrays.hashCode(blockIds);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("blockIds", Arrays.toString(blockIds))
                .add("blockIdsToRelease", Arrays.toString(blockIdsToRelease))
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof PrepareBlocks) {
            PrepareBlocks o = (PrepareBlocks) obj;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Arrays.equals(blockIds, o.blockIds)
                    && Arrays.equals(blockIdsToRelease, o.blockIdsToRelease);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + Encoders.StringArrays.encodedLength(blockIds)
                + Encoders.StringArrays.encodedLength(blockIdsToRelease);
    }

    @Override
    protected Type type() {
        return Type.PREPARE_BLOCKS;
    }

    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        Encoders.StringArrays.encode(buf, blockIds);
        Encoders.StringArrays.encode(buf, blockIdsToRelease);
    }

    public static PrepareBlocks decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        String[] blockIds = Encoders.StringArrays.decode(buf);
        String[] releaseBlocks = Encoders.StringArrays.decode(buf);
        return new PrepareBlocks(appId, execId, blockIds, releaseBlocks);
    }
}