package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.client.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.client.dto.zk.LeaderData;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.StringUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;
import io.netty.channel.ChannelFuture;

import java.util.List;

/**
 * 从leader更新Brokers列表。
 * 低频率
 */
public class UpdateBrokersTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
               Dto.Frame.Builder builder= Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientRequestLeaderSendBrokers).setSource(NodeService.getConfig().getAddress());
                ByteStringPack pack=new ByteStringPack();
                boolean ret=NettyMsgService.sendSyncMsgWithCount(builder,NodeService.CURRENTNODE.getClusterLeader().getClient(),1,0,pack);
                if(ret){
                   StringListDto.StringList list=StringListDto.StringList.parseFrom(pack.getRespbody()) ;
                   List<String> brokers=list.getListList();
                   NodeService.CURRENTNODE.getBrokers().clear();
                   if(brokers!=null){
                       brokers.forEach(x->{
                           NodeService.CURRENTNODE.getBrokers().add(new BaseNode(x));
                       });
                   }
                }
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                Thread.sleep(NodeService.getConfig().getUpdateBrokersTime());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

}
