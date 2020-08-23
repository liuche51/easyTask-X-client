package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.ClusterService;
import com.github.liuche51.easyTaskX.client.cluster.VoteBroker;
import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.client.util.DateUtils;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.github.liuche51.easyTaskX.client.zk.ZKService;

import javax.swing.event.AncestorEvent;
import java.util.Iterator;
import java.util.Map;

/**
 * 节点对zk的心跳。检查leader是否失效。
 * 失效则进入选举
 */
public class CheckBrokersAliveTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> brokers = ClusterService.CURRENTNODE.getBrokers();
                Iterator<Map.Entry<String, Node>> items = brokers.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    Node oldFollow=item.getValue();
                    String path = StringConstant.CHAR_SPRIT+ StringConstant.SERVER+StringConstant.CHAR_SPRIT + item.getValue().getAddress();
                    ZKNode node = ZKService.getDataByPath(path);
                    if (node == null)//防止broker节点已经不在zk。不需要重新选。
                    {
                        log.info("CheckBrokersAliveTask():broker is not exist in zk.");
                        items.remove();
                        continue;
                    }
                    //如果最后心跳时间超过60s，则直接删除该节点信息。并从自己的leader集合中移除掉
                    if (DateUtils.isGreaterThanLoseTime(node.getLastHeartbeat(),item.getValue().getClockDiffer().getDifferSecond())) {
                        items.remove();
                        ZKService.deleteNodeByPathIgnoreResult(path);
                    }
                    //如果最后心跳时间超过30s，进入选举新leader流程。并从自己的leader集合中移除掉
                    else if (DateUtils.isGreaterThanDeadTime(node.getLastHeartbeat(),item.getValue().getClockDiffer().getDifferSecond())) {
                        log.info("heartBeatToLeader():start to selectNewLeader");
                        VoteBroker.voteNewBroker(oldFollow);
                    }

                }
            } catch (Exception e) {
                log.error("CheckBrokersAliveTask()", e);
            }
            try {
                Thread.sleep(AnnularQueue.getInstance().getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("CheckBrokersAliveTask()", e);
            }
        }
    }
}
