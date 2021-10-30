package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.task.AnnularQueueTask;
import com.github.liuche51.easyTaskX.client.util.Util;

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
    public String submitAllowWait(Task task) throws Exception {
        while (!NodeService.IS_STARTED) {
            Thread.sleep(1000l);//如果未启动则休眠1s
        }
        return this.submit(task);
    }

    /**
     * 客户端提交任务。如果easyTask组件未启动，则抛出异常
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submit(Task task) throws Exception {
        if (!NodeService.IS_STARTED) throw new Exception("the easyTask-X has not started,please wait a moment!");
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
            BrokerService.submitTask(innerTask);
        }
        AnnularQueueTask.getInstance().submitAddSlice(innerTask);
        return innerTask.getId();
    }
}
