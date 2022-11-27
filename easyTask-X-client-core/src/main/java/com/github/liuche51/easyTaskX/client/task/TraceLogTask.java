package com.github.liuche51.easyTaskX.client.task;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.TraceLog;
import com.github.liuche51.easyTaskX.client.enume.TaskTraceStoreModel;
import com.github.liuche51.easyTaskX.client.util.HttpRequest;
import com.github.liuche51.easyTaskX.client.util.LogUtil;
import com.github.liuche51.easyTaskX.client.util.TraceLogUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 任务跟踪日志入库
 */
public class TraceLogTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                String taskTraceStoreModel = ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel();
                List<TraceLog> traceLogs=new ArrayList<>(1000);
                TraceLogUtil.TASK_TRACE_WAITTO_WRITE.drainTo(traceLogs,1000);
                if(TaskTraceStoreModel.LOCAL.equalsIgnoreCase(taskTraceStoreModel)){

                }else if(TaskTraceStoreModel.EXT.equalsIgnoreCase(taskTraceStoreModel)){
                    String taskTraceExtUrl = ClientService.getConfig().getAdvanceConfig().getTaskTraceExtUrl();
                    HttpRequest.sendPost(taskTraceExtUrl,"logs="+ JSONObject.toJSONString(traceLogs));
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
            try {
                TimeUnit.MILLISECONDS.sleep(ClientService.getConfig().getAdvanceConfig().getTraceLogWriteIntervalTimes());
            } catch (InterruptedException e) {
                LogUtil.error("", e);
            }
        }
    }
}
