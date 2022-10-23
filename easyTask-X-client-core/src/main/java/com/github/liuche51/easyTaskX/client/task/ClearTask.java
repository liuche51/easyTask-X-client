package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.netty.server.handler.BrokerNotifyClientExecuteTaskHandler;
import com.github.liuche51.easyTaskX.client.util.LogUtil;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 负责将待删除任务队列任务发送到服务端
 * 1、一个实例运行，轮询每个服务端Broker队列
 * 2、将任务批量打包交给系统线程池推送至服务端。
 */
public class ClearTask extends TimerTask {
    /**
     * 执行任务幂等性判定临时数据超时清除时间。60秒
     */
    private static long taskIdempotenceTimeout = 1000 * 60;

    @Override
    public void run() {
        while (!isExit()) {
            setLastRunTime(new Date());
            try {
                // -------------------清理任务执行幂等性临时数据---------------------------------------
                Iterator<Map.Entry<String, Long>> iterator = BrokerNotifyClientExecuteTaskHandler.LastReceivedTaskId.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Long> next = iterator.next();
                    if (System.currentTimeMillis() - next.getValue().longValue() > taskIdempotenceTimeout)
                        iterator.remove();
                }
                try {
                    if (new Date().getTime() - getLastRunTime().getTime() < 500)//防止频繁空转
                        TimeUnit.MILLISECONDS.sleep(500L);
                } catch (InterruptedException e) {
                    LogUtil.error("", e);
                }
            } catch (Exception e) {
                LogUtil.error("", e);
            }
        }
    }
}
