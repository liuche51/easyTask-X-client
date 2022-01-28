package com.github.liuche51.easyTaskX.client.netty.server.handler;

import com.github.liuche51.easyTaskX.client.core.Slice;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.task.AnnularQueueTask;
import com.google.protobuf.ByteString;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Client接受响应：Broker通知Client接受执行新任务
 */
public class BrokerNotifyClientExecuteNewTaskHandler extends BaseHandler {
    @Override
    public ByteString process(Dto.Frame frame) throws Exception {
        ScheduleDto.ScheduleList scheduleList = ScheduleDto.ScheduleList.parseFrom(frame.getBodyBytes());
        List<ScheduleDto.Schedule> list = scheduleList.getSchedulesList();
        for (ScheduleDto.Schedule schedule : list) {
            //防止重复添加任务。接口幂等性设计
            if (hasTheSameTask(schedule.getId())) continue;
            InnerTask task = InnerTask.parseFromScheduleDto(schedule);
            task.setBroker(frame.getSource());
            //周期任务，且为非立即执行的，尽可能早点计算其下一个执行时间。免得因为持久化导致执行时间延迟
            if (task.getTaskType().equals(TaskType.PERIOD) && !task.isImmediately()) {
                task.setExecuteTime(InnerTask.getNextExcuteTimeStamp(task.getPeriod(), task.getUnit()));
            }
            AnnularQueueTask.getInstance().submitAddSlice(task);
        }
        return null;
    }

    /**
     * 判断当前任务是否已经存在环形队列中了。
     *
     * @param id
     * @return
     */
    private boolean hasTheSameTask(String id) {
        Slice[] slices = AnnularQueueTask.getInstance().getSlices();
        for (Slice slice : slices) {
            if (slice.getList().containsKey(id))
                return true;
        }
        return false;
    }
}
