package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.EasyTaskConfig;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.dto.Node;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.client.task.*;
import com.github.liuche51.easyTaskX.client.task.TimerTask;
import com.github.liuche51.easyTaskX.client.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
     *
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
        isStarted = true;
    }

    private static void initCurrentNode() throws Exception {
        CURRENTNODE = new Node(Util.getLocalIP(), NodeService.getConfig().getServerPort());
        timerTasks.add(startAnnularQueueTask());
        timerTasks.add(startHeartBeatTask());
        timerTasks.add(startUpdateBrokersTask());
    }

    /**
     * 客户端提交任务。允许线程等待，直到easyTask组件启动完成
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submitAllowWait(Task task) throws Exception {
        while (!isStarted) {
            Thread.sleep(1000l);//如果未启动则休眠1s
        }
        return this.submit(task);
    }

    /**
     * 客户端提交任务。如果easyTask组件未启动，则抛出异常
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submit(Task task) throws Exception {
        if (!isStarted) throw new Exception("the easyTask-X has not started,please wait a moment!");
        InnerTask innerTask = InnerTask.parseFromTask(task);
        innerTask.setId(Util.generateUniqueId());
        String path = task.getClass().getName();
        innerTask.setTaskClassPath(path);
        innerTask.setGroup(NodeService.getConfig().getGroup());
        //周期任务，且为非立即执行的，尽可能早点计算其下一个执行时间。免得因为持久化导致执行时间延迟
        if (innerTask.getTaskType().equals(TaskType.PERIOD) && !innerTask.isImmediately()) {
            innerTask.setExecuteTime(InnerTask.getNextExcuteTimeStamp(innerTask.getPeriod(), innerTask.getUnit()));
        }
        //一次性立即执行的任务不需要持久化服务
        if (!(innerTask.getTaskType().equals(TaskType.ONECE) && innerTask.isImmediately())) {
            //以下两行代码不要调换顺序，否则可能发生任务已经执行完成，而任务尚未持久化，导致无法执行删除持久化的任务风险
            //为保持数据一致性。应该先提交任务，成功后再执行任务。否则可能出现任务已经执行，持久化却失败了。导致异常情况
            BrokerService.submitTask(innerTask);
        }
        AnnularQueueTask.getInstance().submitAddSlice(innerTask);
        return innerTask.getId();
    }

    /**
     * 节点对leader的心跳。
     */
    public static TimerTask startHeartBeatTask() {
        HeartbeatsTask task = new HeartbeatsTask();
        task.start();
        return task;
    }

    /**
     * 启动任务执行器
     */
    public static TimerTask startAnnularQueueTask() {
        AnnularQueueTask task = AnnularQueueTask.getInstance();
        task.start();
        return task;
    }

    /**
     * 启动更新Broker注册表信息
     */
    public static TimerTask startUpdateBrokersTask() {
        UpdateBrokersTask task = new UpdateBrokersTask();
        task.start();
        return task;
    }
}
