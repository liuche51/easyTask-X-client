package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.github.liuche51.easyTaskX.client.util.StringUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;

public class ClientUtil {

    /**
     * 询问leader是否自己还处于存活状态
     * 1、如果自己因为重启，还处于心跳周期内。则leader认为还处于存活状态。这样就不用重新以新节点方式加入集群。也不用删除旧数据了。
     * @return
     */
    public static boolean isAliveInCluster() {
        try {
            LeaderData node = ZKService.getLeaderData(false);
            if (node != null && !StringUtils.isNullOrEmpty(node.getHost())) {//获取leader信息成功
                BaseNode leader = new BaseNode(node.getHost(), node.getPort());
                ClientService.CLUSTER_LEADER=leader;
                Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowNotifyLeaderHasRestart)
                        .setSource(StringConstant.BROKER);
                ByteStringPack pack = new ByteStringPack();
                boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, leader.getClient(), ClientService.getConfig().getAdvanceConfig().getTryCount(), 5, pack);
                if (!ret) {
                    LogUtil.error("Client因重启询问leader是否自己还处于存活状态。失败！");
                } else {
                    String result = pack.getRespbody().toStringUtf8();
                    if (StringConstant.ALIVE.equals(result)) {
                        return true;
                    }
                }
            }

        } catch (Exception e) {
            LogUtil.error("", e);
        }
        return false;
    }
}
