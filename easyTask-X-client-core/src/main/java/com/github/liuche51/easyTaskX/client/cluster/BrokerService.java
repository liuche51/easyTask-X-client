package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskRequest;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
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
     * 2、由专门的一个线程负责轮询所有队列，批量方式打包成线程池任务推送至服务端
     * 3、服务端收到任务后，立即返回信息。等任务完成同步后，异步返回给客户端
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<SubmitTaskRequest>> WAIT_SEND_TASK = new ConcurrentHashMap<>(2);
    /**
     * 等待删除的任务队列。
     * 1、每个broker一个单独的队列
     * 2、由专门的一个线程负责轮询所有队列，批量方式打包成线程池任务推送至服务端
     * 3、服务端收到任务后，立即返回信息。等任务完成同步后，异步返回给客户端
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<String>> WAIT_DELETE_TASK = new ConcurrentHashMap<>(2);

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
    public static void submitTask(InnerTask task, int submitModel, int timeout) throws Exception {
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
        ScheduleDto.Schedule schedule = task.toScheduleDto(submitModel);
        addWAIT_SEND_TASK(new SubmitTaskRequest(schedule, selectedNode.getAddress(), timeout));
    }

    /**
     * 往所有broker发送队列里添加任务
     *
     * @param submitTaskRequest
     */
    public static void addWAIT_SEND_TASK(SubmitTaskRequest submitTaskRequest) throws Exception {
        LinkedBlockingQueue<SubmitTaskRequest> queue = WAIT_SEND_TASK.get(submitTaskRequest.getSubmitBroker());
        if (queue == null) {// 防止数据不一致导致未能正确添加Broker的队列
            WAIT_SEND_TASK.put(submitTaskRequest.getSubmitBroker(), new LinkedBlockingQueue<SubmitTaskRequest>(NodeService.getConfig().getWaitSendTaskCount()));
            queue = WAIT_SEND_TASK.get(submitTaskRequest.getSubmitBroker());
        }
        boolean offer = queue.offer(submitTaskRequest, submitTaskRequest.getTimeOut(), TimeUnit.SECONDS);//插入元素，如果队列满阻塞，超时后返回false，否则返回true
        if (offer == false) {
            throw new Exception("Queue WAIT_SEND_TASK is full.Please wait a moment try agin.");
        } else {
            if (submitTaskRequest.getSchedule().getSubmitModel() == 0) {
                return;
            } else {
                SubmitTaskResult lock = new SubmitTaskResult();
                TASK_SYNC_BROKER_STATUS.put(submitTaskRequest.getSchedule().getId(), lock);
                synchronized (lock) {
                    lock.wait(submitTaskRequest.getTimeOut() * 1000);//等待服务端提交任务最终成功后唤醒
                    SubmitTaskResult submitTaskResult = TASK_SYNC_BROKER_STATUS.get(submitTaskRequest.getSchedule().getId());
                    if (submitTaskResult != null) {
                        switch (submitTaskResult.getStatus()) {
                            case 0://如果线程被唤醒，判断下任务的状态。为0表示超时自动被唤醒的。
                                BrokerService.addWAIT_DELETE_TASK(submitTaskRequest.getSubmitBroker(), submitTaskRequest.getSchedule().getId());
                                throw new Exception("Task submit timeout,Please try agin.");
                            case 1://服务端已经反馈任务提交成功
                                return;
                            case 9:
                                throw new Exception("Task submit failed,Please try agin." + submitTaskResult.getError());
                            default:
                                break;
                        }
                        TASK_SYNC_BROKER_STATUS.remove(submitTaskRequest.getSchedule().getId());
                    } else {
                        throw new Exception("Task submit failed,Please try agin.");
                    }
                }
            }
        }
    }

    /**
     * 往所有broker发送队列里添加任务
     *
     * @param taskId
     */
    public static void addWAIT_DELETE_TASK(String broker, String taskId) {
        LinkedBlockingQueue<String> queue = WAIT_DELETE_TASK.get(broker);
        if (queue == null) {// 防止数据不一致导致未能正确添加Broker的队列
            WAIT_DELETE_TASK.put(broker, new LinkedBlockingQueue<String>(NodeService.getConfig().getWaitSendTaskCount()));
            queue = WAIT_DELETE_TASK.get(broker);
        }
        try {
            boolean offer = queue.offer(taskId, NodeService.getConfig().getTimeOut(), TimeUnit.SECONDS);//插入元素，如果队列满阻塞，超时后返回false，否则返回true
            if (offer == false) {
                log.error("Queue WAIT_DELETE_TASK is full.");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }

    }
}
