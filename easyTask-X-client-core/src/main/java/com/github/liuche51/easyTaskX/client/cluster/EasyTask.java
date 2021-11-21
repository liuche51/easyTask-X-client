package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.dto.BaseNode;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.Dto;
import com.github.liuche51.easyTaskX.client.dto.proto.StringListDto;
import com.github.liuche51.easyTaskX.client.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.client.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.client.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.client.task.AnnularQueueTask;
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
        while (!NodeService.IS_STARTED) {
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
        if (!NodeService.IS_STARTED) throw new Exception("the easyTask-X has not started,please wait a moment!");
        if (submitModel < 0 || submitModel > 2) throw new Exception("submitModel can set be (0,1,2)!");
        if (timeout <= 0) throw new Exception("timeout mustbe >0!");
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
            BrokerService.submitTask(innerTask, submitModel, timeout);
        }
        AnnularQueueTask.getInstance().submitAddSlice(innerTask);
        return innerTask.getId();
    }

    /**
     * 删除任务。全局通缉模式
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
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.ClientNotifyLeaderDeleteTask).setSource(NodeService.getConfig().getAddress())
                .setBodyBytes(builder0.build().toByteString());
        boolean ret = NettyMsgService.sendSyncMsgWithCount(builder, NodeService.CURRENT_NODE.getClusterLeader().getClient(), 1, 0, null);
        return ret;
    }
}
