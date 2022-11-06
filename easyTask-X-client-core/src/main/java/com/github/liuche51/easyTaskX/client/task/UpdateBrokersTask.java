package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.ByteStringPack;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.Util;

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
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientRequestLeaderSendBrokers).setSource(ClientService.getConfig().getAddress());
                ByteStringPack pack=new ByteStringPack();
                boolean ret=NettyMsgService.sendSyncMsgWithCount(builder, ClientService.CLUSTER_LEADER.getClient(),1,0,pack);
                if(ret){
                   StringListDto.StringList list=StringListDto.StringList.parseFrom(pack.getRespbody()) ;
                   List<String> brokers=list.getListList();
                   ClientService.BROKERS.clear();
                   if(brokers!=null){
                       brokers.forEach(x->{
                           ClientService.BROKERS.add(new BaseNode(x));
                       });
                   }
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                Thread.sleep(ClientService.getConfig().getAdvanceConfig().getUpdateBrokersTime());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }

}
