package com.github.liuche51.easyTaskX.client.core;

import com.github.liuche51.easyTaskX.client.dto.InnerTask;
import com.github.liuche51.easyTaskX.client.util.LogUtil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ProxyFactory {
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
                        LogUtil.debug("任务:{} 代理执行开始", id);
                        try {
                            return method.invoke(target, args);
                        } catch (Exception e) {
                            LogUtil.error("target proxy method execute exception！task.id=" + id, e);
                            throw e;
                        } finally {
                            LogUtil.debug("任务:{} 代理执行结束", id);
                        }
                    }
                }
        );
    }

}
