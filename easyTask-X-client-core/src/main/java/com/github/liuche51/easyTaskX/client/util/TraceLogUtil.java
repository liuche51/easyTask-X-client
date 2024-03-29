package com.github.liuche51.easyTaskX.client.util;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.TraceLog;
import com.github.liuche51.easyTaskX.client.enume.TaskTraceStoreModel;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 任务跟踪日志专用工具类
 */
public class TraceLogUtil {
    /**
     * 本地任务跟踪日志
     */
    public static ConcurrentHashMap<String, List<String>> TASK_TRACE_LOGS=new ConcurrentHashMap<>();
    /**
     * 等待写本地库或外部HTTP调用的跟踪日志
     */
    public static LinkedBlockingQueue<TraceLog> TASK_TRACE_WAITTO_WRITE = new LinkedBlockingQueue<>(ClientService.getConfig().getAdvanceConfig().getWaitSendTaskCount());


    /**
     * 任务跟踪日志专用
     * @param taskId
     * @param s
     * @param o
     */
    public static void trace(String taskId, String s, Object... o) {
        if (ClientService.getConfig().getAdvanceConfig().isDebug()) {
            String nodeAddress="";
            try {
                nodeAddress=ClientService.getConfig().getAddress();
            } catch (Exception e) {
                e.printStackTrace();
            }
            String taskTraceStoreModel = ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel();
            if (null == taskTraceStoreModel)
                LogUtil.info("TaskId=" + taskId + ":" + s, o);
            else if (TaskTraceStoreModel.MEMORY.equalsIgnoreCase(taskTraceStoreModel)) {
                List<String> logs = TASK_TRACE_LOGS.get(taskId);
                if (logs == null) {
                    logs = new LinkedList<>();
                    TASK_TRACE_LOGS.put(taskId, logs);
                }
                FormattingTuple ft = MessageFormatter.arrayFormat(s, o);
                logs.add("happened at " + nodeAddress + " " + ft.getMessage());
            } else if (TaskTraceStoreModel.LOCAL.equalsIgnoreCase(taskTraceStoreModel) || TaskTraceStoreModel.EXT.equalsIgnoreCase(taskTraceStoreModel)) {
                FormattingTuple ft = MessageFormatter.arrayFormat(s, o);
                TraceLog traceLog = new TraceLog(taskId, nodeAddress + " " + ft.getMessage());
                // 如果日志缓冲队列已满，则丢弃，不阻塞主线程。写入重要系统错误
                if (!TASK_TRACE_WAITTO_WRITE.offer(traceLog)) {
                    LogUtil.info("队列 TASK_TRACE_WAITTO_WRITE is full.", "com.github.liuche51.easyTaskX.util.LogUtil.trace");
                }
            }
        }
    }
}
