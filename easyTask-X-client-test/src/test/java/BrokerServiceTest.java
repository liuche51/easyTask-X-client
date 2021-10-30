import com.github.liuche51.easyTaskX.client.dto.BaseNode;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class BrokerServiceTest {
    static CopyOnWriteArrayList<String> list = new CopyOnWriteArrayList<String>();
    public static ConcurrentHashMap<String, BaseNode> TASK_SYNC_BROKER_STATUS = new ConcurrentHashMap<>();
    @org.junit.Test
    public void test() {
        try {
            //线程1.模拟集合初始化以及定时跟新数据
            Thread th1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {

                        try {
                            list.add(String.valueOf(System.currentTimeMillis()));
                            Thread.sleep(10l);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            });
            th1.start();
            Thread.sleep(3000l);//等待线程1，集合初始化一些数据
            //线程2.模拟随机获取列表元素。
            Thread th2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            //Thread.sleep(10l);
                            //CopyOnWriteArrayList<String> list2=list;
                            Random random = new Random();
                            int index = random.nextInt(list.size());//随机生成的随机数范围就变成[0,size)。
                            String ret = list.get(index);
                            System.out.println("list.size()=" + list.size() + ";index=" + index);
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }

                    }
                }
            });
            th2.start();
            //线程3.模拟定时更新Broker列表。使用新集合对象替换旧的集合对象
            Thread th3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(1000l);
                            CopyOnWriteArrayList<String> list2=new CopyOnWriteArrayList<String>();
                            //list.clear();//这种方式，会导致线程2异常
                            for (int i = 0; i < 50; i++) {
                                list2.add(String.valueOf(System.currentTimeMillis()));
                            }
                            list=list2;
                            System.out.println(" list=list2;");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            break;
                        }

                    }
                }
            });
            th3.start();
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @org.junit.Test
    public void test2(){
        BaseNode baseNode=new BaseNode("11111",11);
        try {
           synchronized (baseNode){
               baseNode.wait(5000L);
           }
           System.out.println("end");
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}

