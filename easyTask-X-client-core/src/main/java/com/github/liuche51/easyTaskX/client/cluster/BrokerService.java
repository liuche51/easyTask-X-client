package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskRequest;
import com.github.liuche51.easyTaskX.client.dto.SubmitTaskResult;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.client.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.client.util.TraceLogUtil;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BrokerService {
    /**
     * 等待发送任务队列。
     * 1、每个broker一个单独的队列
     * 2、由专门的一个线程负责轮询所有队列，批量方式打包成线程池任务推送至服务端
     * 3、服务端收到任务后，立即返回信息。等任务完成同步后，异步返回给客户端
     */
    public static ConcurrentHashMap<String, LinkedBlockingQueue<SubmitTaskRequest>> WAIT_SEND_TASK = new ConcurrentHashMap<>(2);


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
    public static void submitTask(InnerTask task, int submitModel, int timeout, TaskFuture future) throws Exception {
        CopyOnWriteArrayList<BaseNode> brokers = ClientService.BROKERS;
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
        addWAIT_SEND_TASK(new SubmitTaskRequest(schedule, selectedNode.getAddress(), timeout), future);
        TraceLogUtil.trace(schedule.getId(),"任务已提交本地发送队列，等待发送到服务端");
    }

    /**
     * 往所有broker发送队列里添加任务
     *
     * @param submitTaskRequest
     * @param future            future接口调用
     */
    private static void addWAIT_SEND_TASK(SubmitTaskRequest submitTaskRequest, TaskFuture future) throws Exception {
        LinkedBlockingQueue<SubmitTaskRequest> queue = WAIT_SEND_TASK.get(submitTaskRequest.getSubmitBroker());
        if (queue == null) {// 防止数据不一致导致未能正确添加Broker的队列
            WAIT_SEND_TASK.put(submitTaskRequest.getSubmitBroker(), new LinkedBlockingQueue<SubmitTaskRequest>(ClientService.getConfig().getAdvanceConfig().getWaitSendTaskCount()));
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
                        try {
                            switch (submitTaskResult.getStatus()) {
                                case SubmitTaskResultStatusEnum
                                        .WAITING://如果线程被唤醒，判断下任务的状态。为0表示超时自动被唤醒的。
                                    submitTaskResult.setStatus(SubmitTaskResultStatusEnum.FAILED);//最终将超时状态的结果设置为失败状态。
                                throw new Exception("Task submit timeout,Please try agin.");
                                case SubmitTaskResultStatusEnum.SUCCEED://服务端已经反馈任务提交成功
                                    return;
                                case SubmitTaskResultStatusEnum.FAILED:
                                    throw new Exception("Task submit failed,Please try agin." + submitTaskResult.getError());
                                default:
                                    break;
                            }
                            if (future != null)
                                future.setStatus(submitTaskResult.getStatus());
                        } finally {
                            TASK_SYNC_BROKER_STATUS.remove(submitTaskRequest.getSchedule().getId());
                        }
                    } else {
                        throw new Exception("Task submit failed,Please try agin.");
                    }
                }
            }
        }
    }
}
