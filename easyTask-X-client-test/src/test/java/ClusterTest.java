
import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.EasyTaskConfig;
import com.github.liuche51.easyTaskX.client.core.TaskType;
import com.github.liuche51.easyTaskX.client.core.TimeUnit;
import com.github.liuche51.easyTaskX.client.test.task.CusTask1;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 集群测试。模拟三个节点的伪集群
 */
public class ClusterTest {
    private static Logger log = LoggerFactory.getLogger(ClusterTest.class);

    @Test
    public void startNode1() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setServerPort(2021);
            initData(annularQueue,config,"Node1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode2() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = new EasyTaskConfig();
        try {
            initData(annularQueue,config,"Node2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode3() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = new EasyTaskConfig();
        try {
            initData(annularQueue,config,"Node3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode4() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            initData(annularQueue,config,"Node4");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initData(AnnularQueue annularQueue, EasyTaskConfig config, String name) throws Exception {
        config.setZkAddress("127.0.0.1:2181");
        //config.setDeleteZKTimeOunt(500);
        //config.setSelectLeaderZKNodeTimeOunt(500);
        config.setDispatchs(new ThreadPoolExecutor(6, 6, 10000, java.util.concurrent.TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));
        config.setWorkers(new ThreadPoolExecutor(12, 12, 10000, java.util.concurrent.TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));
        annularQueue.start(config);
        CusTask1 task1 = new CusTask1();
        task1.setEndTimestamp(ZonedDateTime.now().plusSeconds(10).toInstant().toEpochMilli());//10秒后执行
        Map<String, String> param = new HashMap<String, String>() {
            {
                put("name", name);
                put("birthday", "1988-1-1");
                put("age", "25");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };
        task1.setParam(param);
        CusTask1 task2 = new CusTask1();
        task2.setPeriod(10);
        task2.setEndTimestamp(ZonedDateTime.now().plusSeconds(10).toInstant().toEpochMilli());
        task2.setTaskType(TaskType.PERIOD);
        task2.setUnit(TimeUnit.SECONDS);
        Map<String, String> param2 = new HashMap<String, String>() {
            {
                put("name", name);
                put("birthday", "1986-1-1");
                put("age", "32");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };
        task2.setParam(param2);
        try {
           // annularQueue.submitAllowWait(task2);//单次提交测试
        } catch (Exception e) {
            e.printStackTrace();
        }
        //JUnit默认是非守护线程启动和Main方法不同。这里防止当前主线程退出导致子线程也退出了
        while (true) {
            Thread.sleep(1000);
            try {
                //annularQueue.submitAllowWait(task1);//多次提交测试
                //annularQueue.submitAllowWait(task2);
            } catch (Exception e) {
                e.printStackTrace();
            }
           // printinfo();
        }
    }

    private void printinfo() {
    }
}
