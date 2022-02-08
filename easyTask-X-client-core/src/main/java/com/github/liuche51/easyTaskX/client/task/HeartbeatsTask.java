package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.StringUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;
import io.netty.channel.ChannelFuture;

/**
 * 节点对leader的心跳。
 */
public class HeartbeatsTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                BaseNode leader = ClientService.CLUSTER_LEADER;
                if (leader == null) {//启动时还没获取leader信息，所以需要去zk获取
                    LeaderData node = ZKService.getLeaderData(false);
                    if (node != null && !StringUtils.isNullOrEmpty(node.getHost())) {//获取leader信息成功
                        leader = new BaseNode(node.getHost(), node.getPort());
                        ClientService.CLUSTER_LEADER=leader;
                    }
                } else {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowHeartbeatToLeader).setSource(ClientService.CURRENT_NODE.getAddress())
                            .setBody("Clinet");//客户端节点
                    ChannelFuture future = NettyMsgService.sendASyncMsg(leader.getClient(), builder.build());//这里使用异步即可。也不需要返回值
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                Thread.sleep(ClientService.getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }

}
