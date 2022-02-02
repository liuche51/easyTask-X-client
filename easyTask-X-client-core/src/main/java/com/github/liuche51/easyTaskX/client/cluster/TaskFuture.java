package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.client.util.StringConstant;

public class TaskFuture {
    /**
     * 任务ID
     */
    private String id;
    /**
     * 反馈状态
     */
    private int bakStatus=0;
    private String error= StringConstant.EMPTY;
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getBakStatus() {
        return bakStatus;
    }

    public void setBakStatus(int bakStatus) {
        this.bakStatus = bakStatus;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String get() {
        return null;
    }
}
