package com.github.liuche51.easyTaskX.client.dto;

import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.core.TimeUnit;

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

}
