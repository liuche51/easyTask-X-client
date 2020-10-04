package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.EasyTaskConfig;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.Node;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.client.task.*;
import com.github.liuche51.easyTaskX.client.task.TimerTask;
import com.github.liuche51.easyTaskX.client.util.DateUtils;
import com.github.liuche51.easyTaskX.client.util.Util;
import com.github.liuche51.easyTaskX.client.zk.ZKService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NodeService {
    private static Logger log = LoggerFactory.getLogger(NodeService.class);
    private static EasyTaskConfig config = null;
    private static volatile boolean isStarted = false;//是否已经启动
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
    public static EasyTaskConfig getConfig() {
        return config;
    }

    public static void setConfig(EasyTaskConfig config) {
        NodeService.config = config;
    }
    /**
     * 启动节点。
     * 线程互斥
     * @param config
     * @throws Exception
     */
    public static synchronized void start(EasyTaskConfig config) throws Exception {
        //避免重复执行
        if (isStarted)
            return;
        if (config == null)
            throw new Exception("config is null,please set a EasyTaskConfig!");
        EasyTaskConfig.validateNecessary(config);
        NodeService.config = config;
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        initCurrentNode();//初始化本节点的集群服务
        isStarted=true;
    }

    public static void initCurrentNode() throws Exception {
        CURRENTNODE = new Node(Util.getLocalIP(), AnnularQueue.getInstance().getConfig().getServerPort());
        timerTasks.add(startAnnularQueueTask());
        timerTasks.add(startHeartBeat());
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
        ConcurrentHashMap<String, BaseNode> brokers = CURRENTNODE.getBrokers();
        Iterator<Map.Entry<String, BaseNode>> items = brokers.entrySet().iterator();
        BaseNode selectedNode = null;
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
        boolean ret = NodeUtil.sendSyncMsgWithCount(selectedNode.getClientWithCount(1), builder.build(), 1);
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
            BaseNode broker = CURRENTNODE.getBrokers().get(brokerAddress);
            if (broker != null) {
                Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.CLIENT_DELETE_TASK).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                        .setBody(taskId);
                boolean ret = NodeUtil.sendSyncMsgWithCount(broker.getClientWithCount(1), builder.build(), 1);
                return ret;
            }
        } catch (Exception e) {
            log.error("deleteTask()-> exception!", e);
        }
        return false;
    }

    /**
     * 节点对leader的心跳。
     */
    public static TimerTask startHeartBeat() {
        HeartbeatsTask task = new HeartbeatsTask();
        task.start();
        return task;
    }

    /**
     * 启动任务执行器
     */
    public static TimerTask startAnnularQueueTask() {
        AnnularQueueTask task = new AnnularQueueTask();
        task.start();
        return task;
    }
}
