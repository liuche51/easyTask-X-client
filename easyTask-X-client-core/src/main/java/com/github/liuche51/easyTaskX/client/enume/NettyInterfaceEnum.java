package com.github.liuche51.easyTaskX.client.enume;

public class NettyInterfaceEnum {
    /**
     * 客户端提交任务
     */
    public static final String ClientNotifyBrokerSubmitTask="ClientNotifyBrokerSubmitTask";
    /**
     * 客户端删除任务
     */
    public static final String ClientNotifyBrokerDeleteTask="ClientNotifyBrokerDeleteTask";
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
}
