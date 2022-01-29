package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;

public class TaskFuture {
    /**
     * 任务ID
     */
    private String id;
    /**
     * 是否还在等待反馈结果中
     */
    private boolean isWaiting=true;
    /**
     * 反馈状态
     */
    private int bakStatus=0;
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isWaiting() {
        return isWaiting;
    }

    public void setWaiting(boolean waiting) {
        isWaiting = waiting;
    }

    public String get() {
        return null;
    }
}
