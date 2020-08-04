package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.task.BrokerClockAdjustTask;
import com.github.liuche51.easyTaskX.client.task.HeartbeatsTask;
import com.github.liuche51.easyTaskX.client.task.OnceTask;
import com.github.liuche51.easyTaskX.client.task.TimerTask;
import com.github.liuche51.easyTaskX.client.util.DateUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class ClusterService {
    private static Logger log = LoggerFactory.getLogger(ClusterService.class);
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENTNODE;
    /**
     * 集群一次性任务线程集合。
     * 系统没有重启只是初始化了集群initCurrentNode()。此时也需要立即停止运行的一次性后台任务
     * 需要定时检查其中的线程是否已经运行完，完了需要移除线程对象，释放内存资源
     */
    public static List<OnceTask> onceTasks = new LinkedList<OnceTask>();
    /**
     * 集群定时任务线程集合。
     * 系统没有重启只是初始化了集群initCurrentNode()。此时需要停止之前的定时任务，重新启动新的
     */
    public static List<TimerTask> timerTasks = new LinkedList<TimerTask>();

    public static boolean initCurrentNode() throws Exception {
        CURRENTNODE = new Node(Util.getLocalIP(), AnnularQueue.getInstance().getConfig().getServerPort());
        ZKNode node = new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort());
        node.setCreateTime(DateUtils.getCurrentDateTime());
        node.setLastHeartbeat(DateUtils.getCurrentDateTime());
        ZKService.register(node);
        timerTasks.add(initHeartBeatToZK());
        timerTasks.add(nodeClockAdjustTask());
        return true;
    }

    public static boolean submitTask(Task task) {
        return true;
    }

    public static boolean deleteTask(String taskId) {
        return true;
    }

    /**
     * 节点对zk的心跳。2s一次
     */
    public static TimerTask initHeartBeatToZK() {
        HeartbeatsTask task = new HeartbeatsTask();
        task.start();
        return task;
    }

    /**
     * 通知follows当前Leader位置。异步调用即可
     *
     * @return
     */
    public static boolean notifyBrokerPosition(Node broker, int tryCount) {
        AnnularQueue.getInstance().getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (broker != null) {
                    ClusterUtil.notifyBrokerPosition(broker, tryCount);
                }
            }
        });
        return true;
    }
    /**
     * 启动同步与其他关联节点的时钟差定时任务
     */
    public static TimerTask nodeClockAdjustTask() {
        BrokerClockAdjustTask task=new BrokerClockAdjustTask();
        task.start();
        return task;
    }
    /**
     * 同步与目标主机的时间差
     * @param nodes
     * @return
     */
    public static void syncObjectNodeClockDiffer(List<Node> nodes, int tryCount)
    {
        AnnularQueue.getInstance().getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (nodes != null) {
                    nodes.forEach(x -> {
                        ClusterUtil.syncBrokerClockDiffer(x,tryCount,5);
                    });
                }
            }
        });
    }
}
