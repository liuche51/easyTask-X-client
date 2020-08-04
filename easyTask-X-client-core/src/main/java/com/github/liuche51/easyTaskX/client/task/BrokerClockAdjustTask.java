package com.github.liuche51.easyTaskX.client.task;


import com.github.liuche51.easyTaskX.client.cluster.ClusterService;
import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.ClockDiffer;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 同步与其他关联节点的时钟差定时任务
 */
public class BrokerClockAdjustTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                ConcurrentHashMap<String, Node> leaders = ClusterService.CURRENTNODE.getBrokers();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    ClockDiffer differ = item.getValue().getClockDiffer();
                    dealSyncObjectNodeClockDiffer(item.getValue(), differ);
                }
            } catch (Exception e) {
                log.error("BrokerClockAdjustTask->", e);
            } finally {
                try {
                    Thread.sleep(300000l);//5分钟执行一次
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
        }

    }

    private void dealSyncObjectNodeClockDiffer(Node node, ClockDiffer differ) {
        //如果还没有同步过时钟差或距离上次同步已经过去5分钟了，则重新同步一次
        if (!differ.isHasSync() || ZonedDateTime.now().minusMinutes(5)
                .compareTo(differ.getLastSyncDate()) > 0) {
            ClusterService.syncObjectNodeClockDiffer(Arrays.asList(node), AnnularQueue.getInstance().getConfig().getTryCount());
        }
    }
}
