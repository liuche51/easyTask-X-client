package com.github.liuche51.easyTaskX.client.enume;

public class NettyInterfaceEnum {
    /**
     * 客户端提交任务
     */
    public static final String CLIENT_SUBMIT_TASK="ClientSubmitTask";
    /**
     * 客户端删除任务
     */
    public static final String CLIENT_DELETE_TASK="ClientDeleteTask";
    /**
     * Follow对Leader的心跳接口
     */
    public static final String Heartbeat="Heartbeat";
    /**
     * Client定时任务获取Brokers列表更新
    */
    public static final String ClientUpdateBrokers="ClientUpdateBrokers";
}
