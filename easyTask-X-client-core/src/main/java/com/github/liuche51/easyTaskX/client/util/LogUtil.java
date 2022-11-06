package com.github.liuche51.easyTaskX.client.util;

import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LogUtil {
    protected static final Logger log = LoggerFactory.getLogger(LogUtil.class);
    /**
     * 本地任务跟踪日志
     */
    public static ConcurrentHashMap<String, List<String>> TASK_TRACE_LOGS=new ConcurrentHashMap<>();

    /**
     * 普通info日志。和系统配置保持一直
     *
     * @param s
     * @param o
     */
    public static void info(String s, Object... o) {
        log.info(s, o);
    }

    /**
     * error日志。和系统配置保持一直
     *
     * @param s
     * @param o
     */
    public static void error(String s, Object... o) {
        log.error(s, o);
    }

    /**
     * 专用于详细任务调试时使用。生产上需要关闭。
     *
     * @param s
     * @param o
     */
    public static void debug(String s, Object... o) {
        if (ClientService.getConfig().getAdvanceConfig().isDebug())
            log.info(s, o);
    }

    /**
     * 任务跟踪日志专用
     * @param taskId
     * @param s
     * @param o
     */
    public static void trace(String taskId, String s, Object... o) {
        if (ClientService.getConfig().getAdvanceConfig().isDebug()) {
            if (null == ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())
                log.info("TaskId=" + taskId + ":" + s, o);
            else if("memory".equalsIgnoreCase(ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){
                List<String> logs = TASK_TRACE_LOGS.get(taskId);
                if(logs==null){
                    logs=new LinkedList<>();
                    TASK_TRACE_LOGS.put(taskId,logs);
                }
                FormattingTuple ft = MessageFormatter.arrayFormat(s, o);
                logs.add(ft.getMessage());
            }else if("ext".equalsIgnoreCase(ClientService.getConfig().getAdvanceConfig().getTaskTraceStoreModel())){

            }
        }
    }
}
