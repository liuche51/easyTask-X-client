package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

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
    /**
     * 同步与目标主机的时间差
     * @param node
     * @return
     */
    public static boolean syncBrokerClockDiffer(Node node, int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        Dto.Frame frame = null;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName(NettyInterfaceEnum.SYNC_CLOCK_DIFFER).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(AnnularQueue.getInstance().getConfig().getAddress());
            long start = System.currentTimeMillis();
            frame = NettyMsgService.sendSyncMsg(node.getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                long nodetime = Long.parseLong(result.getBody());
                long end=System.currentTimeMillis();
                long differ=end-nodetime-(end-start)/2;
                long second=differ/1000;
                log.info("Object host["+node.getAddress()+"] clock differ "+second+"s.");
                node.getClockDiffer().setDifferSecond(second);
                node.getClockDiffer().setHasSync(true);
                node.getClockDiffer().setLastSyncDate(ZonedDateTime.now());
                return true;
            }else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("syncObjectNodeClockDiffer() exception!", e);
        }finally {
            tryCount--;
        }
        log.info("syncObjectNodeClockDiffer()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + node.getAddress());
        try {
            Thread.sleep(waiteSecond*1000);
        } catch (InterruptedException e) {
            log.error("",e);
        }
        return syncBrokerClockDiffer(node, tryCount,waiteSecond);
    }
}
