package com.github.liuche51.easyTaskX.client.task;

import com.github.liuche51.easyTaskX.client.cluster.NodeService;
import com.github.liuche51.easyTaskX.client.core.ProxyFactory;
import com.github.liuche51.easyTaskX.client.core.Slice;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.util.Util;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 任务执行器
 * 单例模式
 */
public class AnnularQueueTask extends TimerTask{
    /**
     * 时间分片。
     */
    private Slice[] slices = new Slice[60];
    private static AnnularQueueTask singleton = null;
    public Slice[] getSlices() {
        return slices;
    }
    public static AnnularQueueTask getInstance() {
        if (singleton == null) {
            synchronized (AnnularQueueTask.class) {
                if (singleton == null) {
                    singleton = new AnnularQueueTask();
                }
            }
        }
        return singleton;
    }

    private AnnularQueueTask() {
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new Slice();
        }
    }
    @Override
    public void run() {
        while (!isExit()) {
            int lastSecond = 0;
            while (true) {
                int second = ZonedDateTime.now().getSecond();
                if (second == lastSecond) {
                    try {
                        Thread.sleep(500l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                Slice slice = slices[second];
                //slice.getList().size()数量多时，会非常耗时。生产下需要关闭此处
                // log.debug("已执行时间分片:{}，任务数量:{}", second, slice.getList() == null ? 0 : slice.getList().size());
                lastSecond = second;
                NodeService.getConfig().getDispatchs().submit(new Runnable() {
                    public void run() {
                        ConcurrentSkipListMap<String, InnerTask> list = slice.getList();
                        List<InnerTask> periodSchedules = new LinkedList<>();
                        Iterator<Map.Entry<String, InnerTask>> items = list.entrySet().iterator();
                        while (items.hasNext()) {
                            Map.Entry<String, InnerTask> item = items.next();
                            InnerTask s = item.getValue();
                            //因为计算时有一秒钟内的精度问题，所以判断时当前时间需多补上一秒。这样才不会导致某些任务无法得到及时的执行
                            if (System.currentTimeMillis() + 1000l >= s.getExecuteTime()) {
                                Runnable proxy = (Runnable) new ProxyFactory(s).getProxyInstance();
                                NodeService.getConfig().getWorkers().submit(proxy);
                                if (TaskType.PERIOD.equals(s.getTaskType()))//周期任务需要重新提交新任务
                                    periodSchedules.add(s);
                                list.remove(item.getKey());
                                log.debug("工作任务:{} 已提交执行。所属分片:{}", s.getId(), second);
                            }
                            //因为列表是已经按截止执行时间排好序的，可以节省后面元素的过期判断
                            else break;
                        }
                        submitNewPeriodSchedule(periodSchedules);
                    }
                });
            }
        }
    }
    /**
     * 批量创建新周期任务
     *
     * @param list
     */
    private void submitNewPeriodSchedule(List<InnerTask> list) {
        for (InnerTask schedule : list) {
            try {
                schedule.setExecuteTime(InnerTask.getNextExcuteTimeStamp(schedule.getPeriod(), schedule.getUnit()));
                int slice = AddSlice(schedule);
                log.debug("已重新提交周期任务:{}，所属分片:{}，线程ID:{}", schedule.getId(), slice, Thread.currentThread().getId());
            } catch (Exception e) {
                log.error("submitNewPeriodSchedule exception！", e);
            }
        }
    }

    /**
     * 将任务添加到时间分片中去。
     *
     * @param task
     * @return
     */
    private int AddSlice(InnerTask task) {
        ZonedDateTime time = ZonedDateTime.ofInstant(new Timestamp(task.getExecuteTime()).toInstant(), ZoneId.systemDefault());
        int second = time.getSecond();
        Slice slice = slices[second];
        ConcurrentSkipListMap<String, InnerTask> list2 = slice.getList();
        list2.put(task.getExecuteTime() + "-" + Util.GREACE.getAndIncrement(), task);
        log.debug("已添加类型:{}任务:{}，所属分片:{} 预计执行时间:{} 线程ID:{}", task.getTaskType().name(), task.getId(), time.getSecond(), time.toLocalTime(), Thread.currentThread().getId());
        return second;
    }

    /**
     * 提交任务到时间轮分片
     * 提交到分片前需要做的一些逻辑判断
     *
     * @param task
     * @throws Exception
     */
    public void submitAddSlice(InnerTask task) throws Exception {
        //立即执行的任务，第一次不走时间分片环形队列，直接提交执行。一次性和周期性任务都通过isImmediately判断是否需要立即执行
        if (task.isImmediately()) {
            log.debug("立即执行类工作任务:{}已提交代理执行", task.getId());
            Runnable proxy = (Runnable) new ProxyFactory(task).getProxyInstance();
            NodeService.getConfig().getWorkers().submit(proxy);
            //如果是一次性任务，则不用继续提交到时间分片中了
            if (task.getTaskType().equals(TaskType.ONECE)) {
                return;
            }
            //前面只处理了周期任务非立即执行的情况。这里处理立即执行的情况下。需要重新设置下一个执行周期
            else if (task.getTaskType().equals(TaskType.PERIOD)) {
                task.setExecuteTime(InnerTask.getNextExcuteTimeStamp(task.getPeriod(), task.getUnit()));
            }
        }
        AddSlice(task);
    }

    /**
     * 清空所有任务
     */
    public void clearTask() {
        for (Slice s : slices) {
            s.getList().clear();
        }
    }

    /**
     * 变更任务的所属Broker。
     * 原Broker宕机时
     * @param newBroker
     * @param oldBroker
     */
    public void changeBroker(String newBroker,String oldBroker){
        for(int i=0;i<this.slices.length;i++){
            Slice s=this.slices[i];
            Iterator<Map.Entry<String, InnerTask>> items=s.getList().entrySet().iterator();
            while (items.hasNext()){
                Map.Entry<String, InnerTask> item=items.next();
                InnerTask task=item.getValue();
                if(oldBroker.equals(task.getBroker()))
                    task.setBroker(newBroker);
            }
        }
    }
}
