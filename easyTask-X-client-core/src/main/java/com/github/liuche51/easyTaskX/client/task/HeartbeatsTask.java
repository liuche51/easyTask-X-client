package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.cluster.ClusterService;
import com.github.liuche51.easyTaskX.client.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.client.util.DateUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;

/**
 * 节点对zk的心跳。2s一次
 */
public class HeartbeatsTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                ZKNode node = ZKService.getDataByCurrentNode();
                node.setLastHeartbeat(DateUtils.getCurrentDateTime());
                node.setServerNodes(Util.nodeToZKHost(ClusterService.CURRENTNODE.getServerNodes()));//直接将本地数据覆盖到zk
                ZKService.setDataByCurrentNode(node);
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                Thread.sleep(AnnularQueue.getInstance().getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

}
