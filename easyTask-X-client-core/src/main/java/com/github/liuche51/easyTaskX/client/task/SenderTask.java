package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.BrokerService;
import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskRequest;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 负责将待发送任务队列任务发送到服务端
 * 1、一个实例运行，轮询每个队列
 * 2、将任务批量打包交给系统线程池推送至服务端。
 */
public class SenderTask extends TimerTask {

    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                Collection<LinkedBlockingQueue<SubmitTaskRequest>> queues = BrokerService.WAIT_SEND_TASK.values();
                List<SubmitTaskRequest> batch = new ArrayList<>(10);
                for (LinkedBlockingQueue<SubmitTaskRequest> queue : queues) { // 轮询每个队列
                    queue.drainTo(batch, 10);// 批量获取，为空不阻塞。
                }
                if (batch.size() > 0) {
                    ClientService.getConfig().getClusterPool().submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                ScheduleDto.ScheduleList.Builder builder0 = ScheduleDto.ScheduleList.newBuilder();
                                batch.forEach(x -> {
                                    builder0.addSchedules(x.getSchedule());
                                });
                                Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientSubmitTaskToBroker).setSource(ClientService.getConfig().getAddress())
                                        .setBodyBytes(builder0.build().toByteString());
                                NettyClient client = new BaseNode(batch.get(0).getSubmitBroker()).getClientWithCount(1);
                                boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
                                if (!ret) {
                                    LogUtil.error("SendTaskTask->sendSyncMsgWithCount exception!");
                                }
                            } catch (Exception e) {
                                LogUtil.error("", e);
                            }

                        }
                    });

                } else {
                    try {
                        if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                            TimeUnit.MILLISECONDS.sleep(500L);
                    } catch (InterruptedException e) {
                        LogUtil.error("", e);
                    }
                }

            } catch (Exception e) {
                LogUtil.error("", e);
            }
        }
    }
}
