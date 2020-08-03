package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterUtil {
    private static Logger log = LoggerFactory.getLogger(ClusterUtil.class);
    /**
     * @param broker
     * @param tryCount
     * @return
     */
    public static boolean notifyBrokerPosition(Node broker, int tryCount) {
        if (tryCount == 0) return false;
        final boolean[] ret = {false};
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName(NettyInterfaceEnum.SYNC_CLIENT_POSITION).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(AnnularQueue.getInstance().getConfig().getAddress());
            ChannelFuture future = NettyMsgService.sendASyncMsg(broker.getClient(),builder.build());
            tryCount--;
            future.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        ret[0] = true;
                    }
                }
            });
            if (ret[0])
                return true;
        } catch (Exception e) {
            tryCount--;
            log.error("notifyFollowLeaderPosition.tryCount=" + tryCount, e);
        }
        return ClusterUtil.notifyBrokerPosition(broker, tryCount);
    }
}
