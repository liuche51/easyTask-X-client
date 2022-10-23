package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.EasyTaskConfig;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.ClientNode;
import com.github.liuche51.easyTaskX.client.netty.server.NettyServer;
import com.github.liuche51.easyTaskX.client.task.*;
import com.github.liuche51.easyTaskX.client.task.TimerTask;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClientService {
    private static EasyTaskConfig config = null;
    public static volatile boolean IS_STARTED = false;//是否已经启动
    /**
     * 集群所有可用的brokers
     */
    public static CopyOnWriteArrayList<BaseNode> BROKERS = new CopyOnWriteArrayList<BaseNode>();
    /**
     * leader
     */
    public static BaseNode CLUSTER_LEADER = null;
    /**
     * 当前集群节点的Node对象
     */
    public static ClientNode CURRENT_NODE;

    /**
     * 集群一次性任务线程集合。
     * 系统没有重启只是初始化了集群initCURRENT_NODE()。此时也需要立即停止运行的一次性后台任务
     * 需要定时检查其中的线程是否已经运行完，完了需要移除线程对象，释放内存资源
     */
    public static List<OnceTask> onceTasks = new LinkedList<OnceTask>();
    /**
     * 集群定时任务线程集合。
     * 系统没有重启只是初始化了集群initCURRENT_NODE()。此时需要停止之前的定时任务，重新启动新的
     */
    public static List<TimerTask> timerTasks = new LinkedList<TimerTask>();

    public static EasyTaskConfig getConfig() {
        return config;
    }

    public static void setConfig(EasyTaskConfig config) {
        ClientService.config = config;
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
        if (IS_STARTED)
            return;
        if (config == null)
            throw new Exception("config is null,please set a EasyTaskConfig!");
        EasyTaskConfig.validateNecessary(config);
        ClientService.config = config;
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        initCURRENT_NODE(true);//初始化本节点的集群服务
        IS_STARTED = true;
    }

    /**
     * 初始化当前节点。
     * 1、系统重启或因心网络问题被leader踢出，然后又恢复了
     * 2、需要支持进程没死，然后重新初始化，可反复执行。被leader剔出后以新的节点加入集群
     *
     * @param isFirstStarted 是否首次初始化。进程重启属于首次
     * @return
     */
    private static void initCURRENT_NODE(boolean isFirstStarted) throws Exception {
        clearThreadTask();
        CURRENT_NODE = new ClientNode(Util.getLocalIP(), ClientService.getConfig().getServerPort());
        timerTasks.add(startHeartBeatTask());
        timerTasks.add(startUpdateBrokersTask());
        timerTasks.add(startSenderTask());
        timerTasks.add(startClearTask());
    }
    /**
     * 清理掉所有定时或后台线程任务
     */
    public static void clearThreadTask() {
        timerTasks.forEach(x -> {//先停止目前所有内部定时任务线程工作
            x.setExit(true);
        });
        timerTasks.clear();
        onceTasks.forEach(x -> {
            x.setExit(true);
        });
        onceTasks.clear();
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
     * 启动更新Broker注册表信息
     */
    public static TimerTask startUpdateBrokersTask() {
        UpdateBrokersTask task = new UpdateBrokersTask();
        task.start();
        return task;
    }
    /**
     * 启动负责将待发送任务队列任务发送到服务端
     */
    public static TimerTask startSenderTask() {
        SenderTask task = new SenderTask();
        task.start();
        return task;
    }
    /**
     * 启动负责清理客户端临时数据的任务
     */
    public static TimerTask startClearTask() {
        ClearTask task = new ClearTask();
        task.start();
        return task;
    }
}
