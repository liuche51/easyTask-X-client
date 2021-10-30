package com.github.liuche51.easyTaskX.client.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.core.TimeUnit;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * 内部使用的Task
 */
public class InnerTask {
    /**
     * 任务截止运行时间
     */
    private long executeTime;
    private TaskType taskType = TaskType.ONECE;
    private long period;
    private TimeUnit unit;
    private boolean immediately = false;//是否立即执行
    private String id;
    private String taskClassPath;
    private String group = "Default";//默认分组
    private String source;
    private String broker;//任务所属
    private Map<String, String> param;
    /**
     * 任务提交模式。
     * 0（高性能模式，任务提交至等待发送服务端队列成功即算成功）
     * 1（普通模式，任务提交至服务端Master化成功即算成功）
     * 2（高可靠模式，任务提交至服务端Master和一个Slave成功即算成功）
     */
    private int submit_model = 1;

    /**
     * 任务提交超时时间单。单位秒
     */
    private int submit_timeout;

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
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

    public void setPeriod(long period) {
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

    public void setParam(Map<String, String> param) {
        this.param = param;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTaskClassPath() {
        return taskClassPath;
    }

    public void setTaskClassPath(String taskClassPath) {
        this.taskClassPath = taskClassPath;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public int getSubmit_model() {
        return submit_model;
    }

    public void setSubmit_model(int submit_model) {
        this.submit_model = submit_model;
    }

    public int getSubmit_timeout() {
        return submit_timeout;
    }

    public void setSubmit_timeout(int submit_timeout) {
        this.submit_timeout = submit_timeout;
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
     *
     * @return
     */
    public ScheduleDto.Schedule toScheduleDto() throws Exception {
        ScheduleDto.Schedule.Builder builder = ScheduleDto.Schedule.newBuilder();
        builder.setId(this.getId()).setClassPath(this.getTaskClassPath()).setExecuteTime(this.getExecuteTime())
                .setTaskType(this.getTaskType().name()).setPeriod(this.period).setUnit(this.getUnit().name())
                .setParam(JSONObject.toJSONString(this.getParam())).setSource(NodeService.getConfig().getAddress())
                .setExecuter(this.getBroker()).setSubmitBroker(this.getBroker()).setSubmitModel(this.getSubmit_model())
                .setSubmitTimeout(this.getSubmit_timeout());
        return builder.build();
    }

    /**
     * 从protocol buffer对象转化为Task对象
     *
     * @param schedule
     * @return
     */
    public static InnerTask parseFromScheduleDto(ScheduleDto.Schedule schedule) throws Exception {
        InnerTask task = new InnerTask();
        task.setId(schedule.getId());
        task.setExecuteTime(schedule.getExecuteTime());
        task.setParam(JSONObject.parseObject(schedule.getParam(), Map.class));
        task.setPeriod(schedule.getPeriod());
        task.setTaskType(TaskType.getByValue(schedule.getTaskType()));
        task.setUnit(TimeUnit.getByValue(schedule.getUnit()));
        task.setTaskClassPath(schedule.getClassPath());
        //task.setGroup();
        return task;
    }

    public static InnerTask parseFromTask(Task task) {
        InnerTask innerTask = new InnerTask();
        innerTask.setExecuteTime(task.getExecuteTime());
        innerTask.setTaskType(task.getTaskType());
        innerTask.setPeriod(task.getPeriod());
        innerTask.setUnit(task.getUnit());
        innerTask.setImmediately(task.isImmediately());
        innerTask.setParam(task.getParam());
        innerTask.setSubmit_model(task.getSubmit_model());
        innerTask.setSubmit_timeout(task.getSubmit_timeout());
        return innerTask;
    }
}
