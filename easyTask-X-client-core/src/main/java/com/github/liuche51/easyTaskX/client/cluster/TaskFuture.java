package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.client.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.github.liuche51.easyTaskX.client.util.StringUtils;

public class TaskFuture {
    /**
     * 任务ID
     */
    private String id;
    /**
     * 反馈状态
     */
    private int status=SubmitTaskResultStatusEnum.WAITING;
    private String error= StringConstant.EMPTY;
    /**
     * 超时等待时间。单位秒
     * 1、默认取提交任务里的超时时间+1s，防止比提交任务环节的超时时间还快。
     */
    private long timeout=0;

    public TaskFuture(long timeout) {
        this.timeout = timeout+1;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

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

    public String get() throws Exception {
        if(!StringUtils.isNullOrEmpty(id)){
            return id;
        }else if(this.status== SubmitTaskResultStatusEnum.FAILED){
            throw new Exception(this.error);
        }else if(this.status==SubmitTaskResultStatusEnum.WAITING){
            synchronized (this) {
                this.wait(this.timeout * 1000);//等待服务端提交任务最终成功后唤醒
                get();//重新调用自己一次获取最终结果
            }
        }
        return id;
    }
}
