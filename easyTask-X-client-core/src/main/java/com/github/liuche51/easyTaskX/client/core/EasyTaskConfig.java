package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.util.StringUtils;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 系统配置项
 */
public class EasyTaskConfig {
    /**
     * zk地址。必填 如:127.0.0.1:2181,192.168.1.128:2181
     */
    private String zkAddress;
    /**
     * Netty客户端连接池大小设置。默认3
     */
    private int nettyPoolSize = 3;
    /**
     * 设置当前节点Netty服务端口号。默认2020
     */
    private int serverPort = 2020;
    /**
     * 设置集群Netty通信调用超时时间。默认30秒
     */
    private int timeOut = 30;
    /**
     * 节点对leader的心跳频率。默认5s一次
     */
    private int heartBeat = 5;
    /**
     * 集群节点之间通信失败重试次数。默认2次
     */
    private int tryCount = 2;
    /**
     * 等待发送任务队列最大长度。
     */
    private int waitSendTaskCount = 10000;
    /**
     * 节点分组。不同的服务分组应该不同的。否则可能导致服务执行任务。默认Default
     */
    private String group="Default";
    /**
     * 是否debug模式。设置为TRUE，就可以看到完整日志跟踪信息
     */
    private boolean debug=false;
    /**
     * 是否需要支持任务执行幂等性
     * 1、true，则框架提供支持任务最多执行一次保障。会有性能消耗
     * 2、false，任务执行可能会多次触发，需要用户自行解决
     */
    private boolean taskIdempotence=true;
    /**
     * Folow节点从leader更新注册表信息间隔时间。单位秒。
     */
    private int followUpdateRegeditTime=300;
    /**
     * 从leader更新Brokers列表间隔时间。单位秒。
     */
    private int updateBrokersTime=60*60;
    /**
     * 集群公用程池
     */
    private ExecutorService clusterPool = null;

    /**
     * 环形队列任务调度线程池
     */
    private ExecutorService dispatchs = null;
    /**
     * 环形队列工作任务线程池
     */
    private ExecutorService workers = null;

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public int getNettyPoolSize() {
        return nettyPoolSize;
    }

    public void setNettyPoolSize(int nettyPoolSize) throws Exception {
        if (nettyPoolSize < 1)
            throw new Exception("nettyPoolSize must >1");
        this.nettyPoolSize = nettyPoolSize;
    }

    public String getAddress() throws Exception {
        StringBuffer buffer = new StringBuffer(Util.getLocalIP());
        buffer.append(":").append(getServerPort());
        return buffer.toString();
    }

    public int getServerPort() {
        return serverPort;
    }

    /**
     * set ServerPort，default 2020
     *
     * @param port
     * @throws Exception
     */
    public void setServerPort(int port) throws Exception {
        if (port == 0)
            throw new Exception("ServerPort must not empty");
        this.serverPort = port;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public int getFollowUpdateRegeditTime() {
        return followUpdateRegeditTime;
    }

    public void setFollowUpdateRegeditTime(int followUpdateRegeditTime) {
        this.followUpdateRegeditTime = followUpdateRegeditTime;
    }

    public int getUpdateBrokersTime() {
        return updateBrokersTime;
    }

    public void setUpdateBrokersTime(int updateBrokersTime) {
        this.updateBrokersTime = updateBrokersTime;
    }

    public int getHeartBeat() {
        return heartBeat * 1000;
    }

    public void setHeartBeat(int heartBeat) {
        this.heartBeat = heartBeat;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public int getWaitSendTaskCount() {
        return waitSendTaskCount;
    }

    public void setWaitSendTaskCount(int waitSendTaskCount) {
        this.waitSendTaskCount = waitSendTaskCount;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isTaskIdempotence() {
        return taskIdempotence;
    }

    public void setTaskIdempotence(boolean taskIdempotence) {
        this.taskIdempotence = taskIdempotence;
    }

    public ExecutorService getClusterPool() {
        return clusterPool;
    }

    /**
     * 设置集群总线程池
     *
     * @param clusterPool
     * @throws Exception
     */
    public void setClusterPool(ThreadPoolExecutor clusterPool) throws Exception {
        this.clusterPool = clusterPool;
    }

    public ExecutorService getDispatchs() {
        return dispatchs;
    }

    public void setDispatchs(ExecutorService dispatchs) {
        this.dispatchs = dispatchs;
    }

    public ExecutorService getWorkers() {
        return workers;
    }

    public void setWorkers(ExecutorService workers) {
        this.workers = workers;
    }

    /**
     * 必填项验证
     *
     * @param config
     * @throws Exception
     */
    public static void validateNecessary(EasyTaskConfig config) throws Exception {
        if (StringUtils.isNullOrEmpty(config.zkAddress))
            throw new Exception("zkAddress is necessary!");
        if (config.dispatchs == null)
            config.dispatchs = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        if (config.workers == null)
            config.workers = Executors.newCachedThreadPool();
        if (config.clusterPool == null)
            config.clusterPool = Executors.newCachedThreadPool();
    }
}
