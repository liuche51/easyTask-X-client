package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.BrokerService;
import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 负责将待发送任务队列任务发送到服务端
 * 1、每个Broker对应一个实例任务
 */
public class SendTaskTask extends TimerTask {
    private String broker;

    public SendTaskTask(String broker) {
        this.broker = broker;
    }

    @Override
    public void run() {
        while (!isExit()) {
            try {
                LinkedBlockingQueue<ScheduleDto.Schedule> queue = BrokerService.WAIT_SEND_TASK.get(this.broker);
                if (queue == null) { // 如果当前Broker队列已经被移除，说明Broker被踢出了集群，则终止专属发送任务
                    this.setExit(true);
                    return;
                }
                ScheduleDto.ScheduleList.Builder builder0 = ScheduleDto.ScheduleList.newBuilder();
                for (int i = 0; i < 10; i++) { // 批量发送
                    ScheduleDto.Schedule schedule = queue.take();
                    builder0.addSchedules(schedule);
                }
                Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientSubmitTaskToBroker).setSource(NodeService.getConfig().getAddress())
                        .setBodyBytes(builder0.build().toByteString());
                NettyClient client = new BaseNode(broker).getClientWithCount(1);
                boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
                if (!ret) {
                    log.error("SendTaskTask->sendSyncMsgWithCount exception!");
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }
    }
}
