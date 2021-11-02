package com.github.liuche51.easyTaskX.client.dto;

import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;

/**
 * 客户单待发送任务封装对象
 */
public class SubmitTaskRequest {
    private ScheduleDto.Schedule schedule;
    private String submitBroker;
    private int timeOut;

    public SubmitTaskRequest(ScheduleDto.Schedule schedule, String submitBroker, int timeOut) {
        this.schedule = schedule;
        this.submitBroker = submitBroker;
        this.timeOut = timeOut;
    }

    public ScheduleDto.Schedule getSchedule() {
        return schedule;
    }

    public void setSchedule(ScheduleDto.Schedule schedule) {
        this.schedule = schedule;
    }

    public String getSubmitBroker() {
        return submitBroker;
    }

    public void setSubmitBroker(String submitBroker) {
        this.submitBroker = submitBroker;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }
}
