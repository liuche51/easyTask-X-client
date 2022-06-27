package com.github.liuche51.easyTaskX.client.enume;

public class NettyInterfaceEnum {
    /**
     * 客户端提交任务
     */
    public static final String ClientSubmitTaskToBroker="ClientSubmitTaskToBroker";
    /**
     * 客户端内部删除任务
     */
    public static final String ClientNotifyBrokerDeleteTask="ClientNotifyBrokerDeleteTask";
    /**
     * 客户端全局通缉模式删除任务
     */
    public static final String ClientNotifyLeaderDeleteTask="ClientNotifyLeaderDeleteTask";
    /**
     * Follow对Leader的心跳接口
     */
    public static final String FollowHeartbeatToLeader="FollowHeartbeatToLeader";
    /**
     * Client定时任务获取Brokers列表更新
    */
    public static final String ClientRequestLeaderSendBrokers="ClientRequestLeaderSendBrokers";
    /**
     * leader通知Clinets。Broker发生变更。
     */
    public static final String LeaderNotifyClientBrokerChanged="LeaderNotifyClientBrokerChanged";
    /**
     * Broker通知Client接受执行新任务
     */
    public static final String BrokerNotifyClientExecuteNewTask="BrokerNotifyClientExecuteNewTask";
    /**
     * Broker通知Client提交的任务同步状态结果。
     */
    public static final String BrokerNotifyClientSubmitTaskResult="BrokerNotifyClientSubmitTaskResult";
    /**
     *follow通知leader，已经重新启动了
     */
    public static final String FollowNotifyLeaderHasRestart="FollowNotifyLeaderHasRestart";
}
