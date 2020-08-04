package com.github.liuche51.easyTaskX.client.cluster;

import com.github.liuche51.easyTaskX.client.core.AnnularQueue;
import com.github.liuche51.easyTaskX.client.core.Node;
import com.github.liuche51.easyTaskX.client.dto.zk.ZKNode;
import com.github.liuche51.easyTaskX.client.util.DateUtils;
import com.github.liuche51.easyTaskX.client.util.StringConstant;
import com.github.liuche51.easyTaskX.client.util.exception.VotedException;
import com.github.liuche51.easyTaskX.client.util.exception.VotingException;
import com.github.liuche51.easyTaskX.client.zk.ZKService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 客户单选举Broker
 * 使用多线程互斥机制
 */
public class VoteBroker {
    private static final Logger log = LoggerFactory.getLogger(VoteBroker.class);
    private static volatile boolean voting = false;//选举状态。多线程控制
    private static ReentrantLock lock = new ReentrantLock();//选举互斥锁

    public static boolean isVoting() {
        return voting;
    }

    /**
     * 节点启动初始化挑选broker。
     * 不存在多线程情况，不需要考虑
     *
     * @return
     */
    public static void initSelectBroker() throws Exception {
        List<String> availableFollows = VoteBroker.getAvailableBrokers(null);
        Node broker = VoteBroker.selectBroker(availableFollows);
        if (broker==null) {
            log.info("broker==null,so start to initSelectFollows");
            initSelectBroker();//数量不够递归重新选VoteFollows.selectFollows中
        } else {
            ConcurrentHashMap<String,Node> brokers=new ConcurrentHashMap<>(1);
            brokers.put(broker.getAddress(),broker);
            ClusterService.CURRENTNODE.setBrokers(brokers);
            //通知follows当前Leader位置
            ClusterService.notifyBrokerPosition(broker, AnnularQueue.getInstance().getConfig().getTryCount());
            ClusterService.syncBrokerClockDiffer(Arrays.asList(broker),AnnularQueue.getInstance().getConfig().getTryCount());
        }
    }

    /**
     * 选择新follow
     * leader同步数据失败或心跳检测失败，则进入选新follow程序
     *
     * @param items 是否在迭代器中访问，是则使用迭代器移除元素
     * @return
     */
    public static Node voteNewBroker(Node oldBroker, Iterator<Node> items) throws Exception {
        if (voting) throw new VotingException("cluster is voting new Broker,please retry later.");
        voting = true;
        Node newBroker = null;
        try {
            lock.lock();
            if (ClusterService.CURRENTNODE.getBrokers().contains(oldBroker)) {
                if (items != null) items.remove();//如果使用以下List集合方法移除，会导致下次items.next()方法报错
                log.info("client remove Broker {}", oldBroker.getAddress());
            } else
                ClusterService.CURRENTNODE.getBrokers().remove(oldBroker);//移除失效的follow
            //多线程下，如果follows已经选好，则让客户端重新提交任务。以后可以优化为获取选举后的follow
            if (ClusterService.CURRENTNODE.getBrokers() != null&&ClusterService.CURRENTNODE.getBrokers().size()>0)
                throw new VotedException("client is voted broker,please retry again.");
            List<String> availableFollows = getAvailableBrokers(Arrays.asList(oldBroker.getAddress()));
            newBroker = selectBroker(availableFollows);
            if (newBroker==null)
                voteNewBroker(oldBroker, items);//没选出来递归重新选
            else {
                ClusterService.CURRENTNODE.getBrokers().put(newBroker.getAddress(),newBroker);
            }

        } finally {
            voting = false;//复原选举装填
            lock.unlock();
        }
        if (newBroker == null)
            throw new Exception("client is vote broker failed,please retry later.");
        //通知follows当前Leader位置
        ClusterService.notifyBrokerPosition(newBroker, AnnularQueue.getInstance().getConfig().getTryCount());
        ClusterService.syncBrokerClockDiffer(Arrays.asList(newBroker),AnnularQueue.getInstance().getConfig().getTryCount());
        return newBroker;
    }

    /**
     * 从zk获取可用的Brokers
     *
     * @return
     */
    private static List<String> getAvailableBrokers(List<String> exclude) throws Exception {
        List<String> availableBrokers = ZKService.getChildrenByServerNode();
        //排除旧的失效节点
        if (exclude != null) {
            exclude.forEach(x -> {
                Optional<String> temp1 = availableBrokers.stream().filter(y -> y.equals(x)).findFirst();
                if (temp1.isPresent())
                    availableBrokers.remove(temp1.get());
            });
        }
        if (availableBrokers.size() < 1)//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
        {
            log.info("availableBrokers is not enough! only has {},current own {}", availableBrokers.size(), ClusterService.CURRENTNODE.getBrokers().size());
            Thread.sleep(1000);
            return getAvailableBrokers(exclude);
        } else
            return availableBrokers;
    }

    /**
     * 从可用follows中选择若干个follow
     *
     * @param availableBrokers 可用follows
     */
    private static Node selectBroker(List<String> availableBrokers) throws InterruptedException {
        Node node = null;
        int size = availableBrokers.size();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(availableBrokers.size());//随机生成的随机数范围就变成[0,size)。
            ZKNode node2 = ZKService.getDataByPath(StringConstant.CHAR_SPRIT + StringConstant.SERVER + StringConstant.CHAR_SPRIT + availableBrokers.get(index));
            //如果最后心跳时间超过60s，则直接删除该节点信息。
            if (DateUtils.isGreaterThanLoseTime(node2.getLastHeartbeat())) {
                ZKService.deleteNodeByPathIgnoreResult(StringConstant.CHAR_SPRIT + StringConstant.SERVER + StringConstant.CHAR_SPRIT + availableBrokers.get(index));
            } else if (DateUtils.isGreaterThanDeadTime(node2.getLastHeartbeat())) {
                //如果最后心跳时间超过30s，也不能将该节点作为follow
            } else {
                node = new Node(node2.getHost(), node2.getPort());
            }
            availableBrokers.remove(index);
        }
        if (node == null) Thread.sleep(1000);//此处防止不满足条件时重复高频递归本方法
        return node;
    }
}
