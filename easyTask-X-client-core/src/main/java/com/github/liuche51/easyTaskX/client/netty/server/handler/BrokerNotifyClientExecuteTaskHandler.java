package com.github.liuche51.easyTaskX.client.netty.server.handler;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.core.ProxyFactory;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.google.protobuf.ByteString;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client接受响应：Broker通知Client接受执行新任务
 */
public class BrokerNotifyClientExecuteTaskHandler extends BaseHandler {
    /**
     * 最近接收到的任务ID
     * 1、用于任务接受幂等性设计，防止重复接收任务重复执行
     * 2、可以选择使用框架的幂等性判断，也可以将幂等性交由用户代码自行判断。因为这个东西启用的话，毕竟也比较消耗性能，而有些用户任务本身就可能支持任务重复执行。所以不是必须的
     */
    public static ConcurrentHashMap<String, Long> LastReceivedTaskId = new ConcurrentHashMap<>(500);

    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ScheduleDto.ScheduleList scheduleList = ScheduleDto.ScheduleList.parseFrom(frame.getBodyBytes());
        List<ScheduleDto.Schedule> list = scheduleList.getSchedulesList();
        for (ScheduleDto.Schedule schedule : list) {
            //防止重复添加任务。接口幂等性设计
            if (ClientService.getConfig().isTaskIdempotence()) {
                if (LastReceivedTaskId.containsKey(schedule.getId()))
                    continue;
                LastReceivedTaskId.put(schedule.getId(), new Date().getTime());
            }

            InnerTask innerTask = InnerTask.parseFromScheduleDto(schedule);
            Runnable proxy = (Runnable) new ProxyFactory(innerTask).getProxyInstance();
            ClientService.getConfig().getWorkers().submit(proxy);
        }
        return null;
    }
}
