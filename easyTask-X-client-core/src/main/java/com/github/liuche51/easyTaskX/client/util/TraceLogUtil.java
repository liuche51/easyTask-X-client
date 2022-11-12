package com.github.liuche51.easyTaskX.client.util;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.exception.EasyTaskException;
import com.github.liuche51.easyTaskX.client.exception.ExceptionCode;
import com.github.liuche51.easyTaskX.client.ext.TaskTraceExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务跟踪日志专用工具类
 */
public class TraceLogUtil {
    protected static final Logger log = LoggerFactory.getLogger(TraceLogUtil.class);
    /**
     * 本地任务跟踪日志
     */
    public static ConcurrentHashMap<String, List<String>> TASK_TRACE_LOGS=new ConcurrentHashMap<>();

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
            if (null == ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())
                log.info("TaskId=" + taskId + ":" + s, o);
            else if("memory".equalsIgnoreCase(ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                List<String> logs = TASK_TRACE_LOGS.get(taskId);
                if(logs==null){
                    logs=new LinkedList<>();
                    TASK_TRACE_LOGS.put(taskId,logs);
                }
                FormattingTuple ft = MessageFormatter.arrayFormat(s, o);
                logs.add("happened at "+nodeAddress+" "+ft.getMessage());
            }else if("ext".equalsIgnoreCase(ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                TaskTraceExt taskTraceExt = ClientService.getConfig().getAdvanceConfig().getTaskTraceExt();
                if(taskTraceExt!=null){
                    taskTraceExt.trace(taskId,nodeAddress,s,o);
                }else{
                    try {
                        throw new EasyTaskException(ExceptionCode.TaskTraceExt_NotFind,"config item taskTraceExt not find.");
                    } catch (EasyTaskException e) {
                        log.error(e.getMessage());
                    }
                }

            }else if("local".equalsIgnoreCase(ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                // Todo 暂时不支持客户端本地存储日志，需要发送到服务端存储
            }
        }
    }
}
