package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.ProxyFactory;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.client.enume.ImmediatelyType;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.enume.SubmitTaskResultStatusEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.util.List;

/**
 * 用户程序编程接口
 */
public class EasyTask {
    /**
     * 客户端提交任务。允许线程等待，直到easyTask组件启动完成
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submitAllowWait(Task task, int submitModel, int timeout) throws Exception {
        while (!ClientService.IS_STARTED) {
            Thread.sleep(1000l);//如果未启动则休眠1s
        }
        return this.submit(task, submitModel, timeout);
    }

    /**
     * 客户端提交任务。如果easyTask组件未启动，则抛出异常
     * submitModel 任务提交模式。
     * 0（高性能模式，任务提交至等待发送服务端队列成功即算成功）
     * 1（普通模式，任务提交至服务端Master化成功即算成功）
     * 2（高可靠模式，任务提交至服务端Master和一个Slave成功即算成功）
     * timeout 任务提交超时时间单。单位秒
     *
     * @param task
     * @param submitModel
     * @return
     * @throws Exception
     * @paratimeoutm timeout
     */
    public String submit(Task task, int submitModel, int timeout) throws Exception {
        return submit(task, submitModel, timeout, null);
    }

    public TaskFuture submitFutrue(Task task, int submitModel, int timeout) throws Exception {
        TaskFuture future = new TaskFuture(timeout);
        ClientService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    submit(task, submitModel, timeout, future);
                } catch (Exception e) {
                    future.setStatus(SubmitTaskResultStatusEnum.FAILED);
                    future.setError(e.getMessage());
                } finally {
                    synchronized (future) {
                        future.notify();//通知其他锁定次对象的线程唤醒继续执行
                    }
                }
            }
        });
        return future;
    }

    private String submit(Task task, int submitModel, int timeout, TaskFuture future) throws Exception {
        if (!ClientService.IS_STARTED) {
            throw new Exception("the easyTask-X has not started,please wait a moment!");
        }
        if (submitModel < 0 || submitModel > 2) throw new Exception("submitModel can set be (0,1,2)!");
        if (timeout <= 0) throw new Exception("timeout mustbe >0!");
        InnerTask innerTask = InnerTask.parseFromTask(task);
        innerTask.setId(Util.generateUniqueId());
        String path = task.getClass().getName();
        innerTask.setTaskClassPath(path);
        innerTask.setGroup(ClientService.getConfig().getAdvanceConfig().getGroup());
        //周期任务，且为非立即执行的，尽可能早点计算其下一个执行时间。免得因为持久化导致执行时间延迟
        if (innerTask.getTaskType().equals(TaskType.PERIOD) && !innerTask.getImmediatelyType().equals(ImmediatelyType.NONE)) {
            innerTask.setExecuteTime(InnerTask.getNextExcuteTimeStamp(innerTask.getPeriod(), innerTask.getUnit()));
        }
        // 所有任务都要先上传服务器然后才能执行
        BrokerService.submitTask(innerTask, submitModel, timeout, future);
        //一次性本地立即执行的任务，直接本地执行
        if (innerTask.getTaskType().equals(TaskType.ONECE) && innerTask.getImmediatelyType().equals(ImmediatelyType.LOCAL)) {
            Runnable proxy = (Runnable) new ProxyFactory(innerTask).getProxyInstance();
            ClientService.getConfig().getAdvanceConfig().getWorkers().submit(proxy);
        }
        if (future != null) {
            future.setId(innerTask.getId());
        }
        return innerTask.getId();
    }

    /**
     * 异步提交模式
     *
     * @param task
     * @param submitModel
     * @param timeout
     * @param listener
     * @throws Exception
     */
    public void submitSync(Task task, int submitModel, int timeout, Listener listener) throws Exception {
        ClientService.getConfig().getAdvanceConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String id = submit(task, submitModel, timeout);
                    listener.success(id);
                } catch (Exception e) {
                    listener.failed(e);
                }
            }
        });
    }

    /**
     * 删除任务。全局通缉模式
     *
     * @param taskIds
     * @return
     * @throws Exception
     */
    public boolean delete(List<String> taskIds) throws Exception {
        StringListDto.StringList.Builder builder0 = StringListDto.StringList.newBuilder();
        taskIds.forEach(x -> {
            builder0.addList(x);
        });
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientNotifyLeaderDeleteTask).setSource(ClientService.getConfig().getAddress())
                .setBodyBytes(builder0.build().toByteString());
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, ClientService.CLUSTER_LEADER.getClient(), 1, 0, null);
        return ret;
    }
}
