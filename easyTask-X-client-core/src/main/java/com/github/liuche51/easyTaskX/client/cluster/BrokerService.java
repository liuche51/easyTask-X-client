package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.task.SendTaskTask;
import com.github.liuche51.easyTaskX.client.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class BrokerService {
    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);
    /**
     * 等待发送任务队列。
     * 1、每个broker一个单独的队列
     * 2、每个队列由一个专属线程负责提交任务到服务端
     * 3、服务端收到任务后，立即返回信息。等任务完成同步后，异步返回给客户端
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<ScheduleDto.Schedule>> WAIT_SEND_TASK = new ConcurrentHashMap<>(2);
    /**
     * 任务同步到服务端状态记录
     */
    public static ConcurrentHashMap<String, SubmitTaskResult> TASK_SYNC_BROKER_STATUS = new ConcurrentHashMap<>();

    /**
     * 提交新任务到集群
     * 如果有多个Broker，则采用随机算法挑选一个
     *
     * @param task
     * @throws Exception
     */
    public static void submitTask(InnerTask task) throws Exception {
        CopyOnWriteArrayList<BaseNode> brokers = NodeService.CURRENT_NODE.getBrokers();
        BaseNode selectedNode = null;
        if (brokers == null || brokers.size() == 0)
            throw new Exception("brokers==null||brokers.size()==0");
        else if (brokers.size() > 1) {
            Random random = new Random();
            int index = random.nextInt(brokers.size());//随机生成的随机数范围就变成[0,size)。
            selectedNode = brokers.get(index);
        } else
            selectedNode = brokers.get(0);
        task.setBroker(selectedNode.getAddress());//将任务所属服务端节点标记一下
        ScheduleDto.Schedule schedule = task.toScheduleDto();
        addWait_Send_Task(schedule);
    }

    /**
     * 删除任务。
     * 已执行完毕的任务，系统自动删除用
     *
     * @param taskId
     * @param brokerAddress
     */
    public static void deleteTask(String taskId, String brokerAddress) {
        NodeService.getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Dto.Frame.Builder builder = Dto.Frame.newBuilder();
                    builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientNotifyBrokerDeleteTask).setSource(NodeService.getConfig().getAddress())
                            .setBody(taskId);
                    NettyClient client = new BaseNode(brokerAddress).getClientWithCount(1);
                    boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, client, 1, 0, null);
                    if (!ret) {
                        log.error("任务:{} 删除失败。", taskId);
                    }
                } catch (Exception e) {
                    log.error("deleteTask()-> exception!", e);
                }
            }
        });
    }

    /**
     * 往所有broker发送队列里添加任务
     *
     * @param schedule
     */
    public static void addWait_Send_Task(ScheduleDto.Schedule schedule) throws Exception {
        LinkedBlockingQueue<ScheduleDto.Schedule> queue = WAIT_SEND_TASK.get(schedule.getSubmitBroker());
        if (queue == null) {// 防止数据不一致导致未能正确添加Broker的队列
            WAIT_SEND_TASK.put(schedule.getSubmitBroker(), new LinkedBlockingQueue<ScheduleDto.Schedule>(NodeService.getConfig().getWaitSendTaskCount()));
            queue=WAIT_SEND_TASK.get(schedule.getSubmitBroker());
            SendTaskTask task = new SendTaskTask(schedule.getSubmitBroker());
            task.start();
        }
        boolean offer = queue.offer(schedule, schedule.getSubmitTimeout(), TimeUnit.SECONDS);
        if (offer == false) {
            throw new Exception("Queue WAIT_SEND_TASK is full.Please wait a moment try agin.");
        } else {
            if (schedule.getSubmitModel() == 0) {
                return;
            } else {
                SubmitTaskResult lock = new SubmitTaskResult();
                TASK_SYNC_BROKER_STATUS.put(schedule.getId(), lock);
                synchronized (lock) {
                    lock.wait(schedule.getSubmitTimeout() * 1000);//等待服务端提交任务最终成功后唤醒
                    SubmitTaskResult submitTaskResult = TASK_SYNC_BROKER_STATUS.get(schedule.getId());
                    if (submitTaskResult != null) {
                        switch (submitTaskResult.getStatus()) {
                            case 0://如果线程被唤醒，判断下任务的状态。为0表示超时自动被唤醒的。
                                deleteTask(schedule.getId(), schedule.getSubmitBroker());//任务有可能后续可能提交成功.触发任务删除操作
                                throw new Exception("Task submit timeout,Please try agin.");
                            case 1://服务端已经反馈任务提交成功
                                return;
                            case 9:
                                throw new Exception("Task submit failed,Please try agin." + submitTaskResult.getError());
                            default:
                                break;
                        }
                        TASK_SYNC_BROKER_STATUS.remove(schedule.getId());
                    } else {
                        throw new Exception("Task submit failed,Please try agin.");
                    }
                }
            }
        }
    }
}
