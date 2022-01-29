package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.BrokerService;
import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskRequest;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 负责将待删除任务队列任务发送到服务端
 * 1、一个实例运行，轮询每个服务端Broker队列
 * 2、将任务批量打包交给系统线程池推送至服务端。
 */
public class DeleteTask extends TimerTask {

    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                List<String> batch = new ArrayList<>(10);
                Iterator<Map.Entry<String, LinkedBlockingQueue<String>>> items = BrokerService.WAIT_DELETE_TASK.entrySet().iterator();
                while (items.hasNext()) {// 轮询每个队列
                    Map.Entry<String, LinkedBlockingQueue<String>> item = items.next();
                    item.getValue().drainTo(batch, 10);// 批量获取，为空不阻塞。
                    if (batch.size() > 0) {
                        NodeService.getConfig().getClusterPool().submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    StringListDto.StringList.Builder builder0 = StringListDto.StringList.newBuilder();
                                    batch.forEach(x -> {
                                        builder0.addList(x);
                                    });
                                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientNotifyBrokerDeleteTask).setSource(NodeService.getConfig().getAddress())
                                            .setBodyBytes(builder0.build().toByteString());
                                    NettyClient client = new BaseNode(item.getKey()).getClientWithCount(1);
                                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
                                    if (!ret) {// 偶发异常，重新入队列重发
                                        batch.forEach(x -> {
                                            BrokerService.addWAIT_DELETE_TASK(item.getKey(), x);
                                        });
                                    }
                                } catch (Exception e) {
                                    LogUtil.error("", e);
                                }
                            }
                        });
                    }
                }
                try {
                    if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                        TimeUnit.MILLISECONDS.sleep(500L);
                } catch (InterruptedException e) {
                    LogUtil.error("", e);
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
        }
    }
}
