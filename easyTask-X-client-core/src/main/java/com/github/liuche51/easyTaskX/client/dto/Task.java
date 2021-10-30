package com.github.liuche51.easyTaskX.client.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.core.TimeUnit;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;

import java.time.ZonedDateTime;
import java.util.Map;

/**
 * 客户端使用Task
 */
public class Task {
    /**
     * 任务截止运行时间
     */
    private long executeTime;
    private TaskType taskType = TaskType.ONECE;
    private long period;
    private TimeUnit unit;
    private boolean immediately = false;//是否立即执行
    private Map<String, String> param;
    /**
     * 任务提交模式。
     * 0（高性能模式，任务提交至等待发送服务端队列成功即算成功）
     * 1（普通模式，任务提交至服务端Master化成功即算成功）
     * 2（高可靠模式，任务提交至服务端Master和一个Slave成功即算成功）
     */
    private int submit_model=1;

    /**
     * 任务提交超时时间单。单位秒
     */
    private int submit_timeout=30;
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

    public void setParam(Map<String, String> param) {
        this.param = param;
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
}
