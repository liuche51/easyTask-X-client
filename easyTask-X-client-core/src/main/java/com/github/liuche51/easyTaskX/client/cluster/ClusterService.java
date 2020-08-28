package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.task.*;
import com.github.liuche51.easyTaskX.client.task.TimerTask;
import com.github.liuche51.easyTaskX.client.util.DateUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
        VoteBroker.initSelectBroker();
        timerTasks.add(initHeartBeatToZK());
        timerTasks.add(nodeClockAdjustTask());
        timerTasks.add(initCheckBrokersAlive());
        return true;
    }

    /**
     * 提交新任务到集群
     * 如果有多个Broker，则采用随机算法挑选一个
     *
     * @param task
     * @throws Exception
     */
    public static void submitTask(Task task) throws Exception {
        ScheduleDto.Schedule schedule = task.toScheduleDto();
        ConcurrentHashMap<String, Node> brokers = CURRENTNODE.getBrokers();
        Iterator<Map.Entry<String, Node>> items = brokers.entrySet().iterator();
        Node selectedNode = null;
        if (brokers.size() > 1) {
            Random random = new Random();
            int index = random.nextInt(brokers.size());//随机生成的随机数范围就变成[0,size)。
            int flag = 0;
            while (items.hasNext()) {
                if (index == flag) {
                    selectedNode = items.next().getValue();
                    break;
                }
            }
        } else
            selectedNode = items.next().getValue();
        task.getTaskExt().setBroker(selectedNode.getAddress());//将任务所属服务端节点标记一下
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.CLIENT_SUBMIT_TASK).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                .setBodyBytes(schedule.toByteString());
        boolean ret = ClusterUtil.sendSyncMsgWithCount(selectedNode.getClientWithCount(1), builder.build(), 1);
        if (!ret) {
            throw new Exception("sendSyncMsgWithCount()->exception! ");
        }
    }

    /**
     * 删除任务。
     * 已执行完毕的任务，系统自动删除用
     *
     * @param taskId
     * @param brokerAddress
     * @throws Exception
     */
    public static boolean deleteTask(String taskId, String brokerAddress) {
        try {
            Node broker = CURRENTNODE.getBrokers().get(brokerAddress);
            if (broker != null) {
                Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.CLIENT_DELETE_TASK).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                        .setBody(taskId);
                boolean ret = ClusterUtil.sendSyncMsgWithCount(broker.getClientWithCount(1), builder.build(), 1);
                return ret;
            }
        } catch (Exception e) {
            log.error("deleteTask()-> exception!", e);
        }
        return false;
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
    public static boolean notifyBrokerClientPosition(Node broker, int tryCount, int waiteSecond) {
        AnnularQueue.getInstance().getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (broker != null) {
                    ClusterUtil.notifyBrokerClientPosition(broker, tryCount, waiteSecond);
                }
            }
        });
        return true;
    }

    /**
     * 启动同步与其他关联节点的时钟差定时任务
     */
    public static TimerTask nodeClockAdjustTask() {
        BrokerClockAdjustTask task = new BrokerClockAdjustTask();
        task.start();
        return task;
    }

    /**
     * 同步与目标主机的时间差
     *
     * @param nodes
     * @return
     */
    public static void syncBrokerClockDiffer(List<Node> nodes, int tryCount) {
        AnnularQueue.getInstance().getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (nodes != null) {
                    nodes.forEach(x -> {
                        ClusterUtil.syncBrokerClockDiffer(x, tryCount, 5);
                    });
                }
            }
        });
    }

    /**
     * 节点对zk的心跳。检查brokers是否失效。
     * 失效则进入选举
     */
    public static TimerTask initCheckBrokersAlive() {
        CheckBrokersAliveTask task = new CheckBrokersAliveTask();
        task.start();
        return task;
    }
}
