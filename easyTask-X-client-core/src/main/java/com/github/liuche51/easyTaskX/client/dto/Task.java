package com.github.liuche51.easyTaskX.client.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.core.TimeUnit;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;

import java.time.ZonedDateTime;
import java.util.Map;

public class Task {
    /**
     * 任务截止运行时间
     */
    private long endTimestamp;
    private TaskType taskType = TaskType.ONECE;
    private long period;
    private TimeUnit unit;
    private boolean immediately=false;//是否立即执行
    private TaskExt taskExt = new TaskExt();
    private Map<String, String> param;


    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) throws Exception {
        if (period <= 0)
            throw new Exception("period cannot less than 0！");
        this.period = period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public boolean isImmediately() {
        return immediately;
    }

    public void setImmediately(boolean immediately) {
        this.immediately = immediately;
    }

    public Map<String, String> getParam() {
        return param;
    }

    public TaskExt getTaskExt() {
        return taskExt;
    }

    public void setParam(Map<String, String> param) {
        this.param = param;
    }

    /**
     * 获取周期性任务下次执行时间。已当前时间为基准计算下次而不是上次截止执行时间
     *
     * @param period
     * @param unit
     * @return
     * @throws Exception
     */
    public static long getNextExcuteTimeStamp(long period, TimeUnit unit) throws Exception {
        switch (unit) {
            case DAYS:
                return ZonedDateTime.now().plusDays(period).toInstant().toEpochMilli();
            case HOURS:
                return ZonedDateTime.now().plusHours(period).toInstant().toEpochMilli();
            case MINUTES:
                return ZonedDateTime.now().plusMinutes(period).toInstant().toEpochMilli();
            case SECONDS:
                return ZonedDateTime.now().plusSeconds(period).toInstant().toEpochMilli();
            default:
                throw new Exception("unSupport TimeUnit type");
        }
    }

    private static TimeUnit getTimeUnit(String unit) {
        switch (unit) {
            case "DAYS":
                return TimeUnit.DAYS;
            case "HOURS":
                return TimeUnit.HOURS;
            case "MINUTES":
                return TimeUnit.MINUTES;
            case "SECONDS":
                return TimeUnit.SECONDS;
            default:
                return null;
        }
    }
    /**
     * 转换为protocol buffer对象
     * @return
     */
    public ScheduleDto.Schedule toScheduleDto() throws Exception {
        ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
        builder.setId(this.getTaskExt().getId()).setClassPath(this.getTaskExt().getTaskClassPath()).setExecuteTime(this.getEndTimestamp())
                .setTaskType(this.getTaskType().name()).setPeriod(this.period).setUnit(this.getUnit().name())
                .setParam(JSONObject.toJSONString(this.getParam())).setSource(NodeService.getConfig().getAddress())
                .setTransactionId("");
        return builder.build();
    }
}
