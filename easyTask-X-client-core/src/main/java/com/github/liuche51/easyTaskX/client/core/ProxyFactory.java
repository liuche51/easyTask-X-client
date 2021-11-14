package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.cluster.BrokerService;
import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProxyFactory {
    private static Logger log = LoggerFactory.getLogger(ProxyFactory.class);
    private InnerTask target;

    public ProxyFactory(InnerTask target) {
        this.target = target;
    }

    public Object getProxyInstance() {
        return Proxy.newProxyInstance(
                target.getClass().getClassLoader(),
                target.getClass().getInterfaces(),
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        String id = target.getId();
                        log.debug("任务:{} 代理执行开始", id);
                        try {
                            return method.invoke(target, args);
                        } catch (Exception e) {
                            log.error("target proxy method execute exception！task.id=" + id, e);
                            throw e;
                        } finally {
                            log.debug("任务:{} 代理执行结束", id);
                            if (target.getTaskType().equals(TaskType.ONECE)) {
                                BrokerService.addWAIT_DELETE_TASK(target.getBroker(), id);
                            }
                        }
                    }
                }
        );
    }

}
