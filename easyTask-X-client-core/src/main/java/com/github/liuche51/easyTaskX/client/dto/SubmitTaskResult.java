package com.github.liuche51.easyTaskX.client.dto;

import com.github.liuche51.easyTaskX.client.util.StringConstant;

/**
 * 提交的任务结果数据分装对象
 */
public class SubmitTaskResult {
    /**
     * 任务状态。
     * 0等待反馈，1反馈成功，9反馈失败。
     */
    private int status=0;
    private String error= StringConstant.EMPTY;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
