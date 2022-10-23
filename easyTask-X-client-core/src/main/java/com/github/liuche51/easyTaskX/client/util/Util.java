package com.github.liuche51.easyTaskX.client.util;


import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTaskX.client.cluster.ClientService;
import com.github.liuche51.easyTaskX.client.dto.Task;
import com.github.liuche51.easyTaskX.client.dto.proto.ScheduleDto;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Util {
    public static AtomicLong GREACE = new AtomicLong(0);

    public static String generateUniqueId() {
        StringBuilder str = new StringBuilder(UUID.randomUUID().toString().replace("-", ""));
        str.append("-");
        str.append(Thread.currentThread().getId());
        return str.toString();
    }
    public static String generateTransactionId() {
        return "T"+generateUniqueId();
    }
    public static String generateIdentityId() {
        return "I"+generateUniqueId();
    }

    public static String getLocalIP() throws Exception {
        if(isLinux()){
            return getLinuxLocalIP();
        }else if(isWindows()){
            return getWindowsLocalIP();
        }else
            throw new Exception("Unknown System Type!Only support Linux and Windows System.");
    }
    /**
     * 获取windows下IP地址
     * 如果在Linux下，则结果都是127.0.0.1
     * @return
     * @throws Exception
     */
    private static String getWindowsLocalIP() throws Exception {
        String ip = InetAddress.getLocalHost().getHostAddress();
        String[] temp = ip.split("/");
        if (temp.length == 1) return temp[0];
        else if (temp.length == 2) return temp[1];
        else return ip;
    }

    /**
     * 获取Linux下的IP地址
     * 如果在windows下，则结果都是127.0.0.1
     * @return
     * @throws SocketException
     */
    private static String getLinuxLocalIP() throws SocketException {
        Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            System.out.println(netInterface.getName());
            Enumeration addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address) {
                    return ip.getHostAddress();
                }
            }
        }
        return null;
    }
    private static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }
    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("windows");
    }

    /**
     * 获取任务来源的拼接字符串
     * @param oldSource
     * @return
     */
    public static String getSource(String oldSource) throws Exception {
        String source=StringConstant.EMPTY;
        if(oldSource==null||oldSource== StringConstant.EMPTY)
            source= ClientService.getConfig().getAddress();
        else
            source= ClientService.getConfig().getAddress()+"<-"+oldSource;
        return source;
    }
    /**
     * 转换为protocol buffer对象
     *
     * @return
     */
    public static ScheduleDto.Schedule toScheduleDto(int submitModel, Task task,String broker) throws Exception {
        ScheduleDto.Schedule.Builder builder = ScheduleDto.Schedule.newBuilder();
        builder.setId(Util.generateUniqueId()).setClassPath(task.getClass().getName()).setExecuteTime(task.getExecuteTime())
                .setTaskType(task.getTaskType().name()).setPeriod(task.getPeriod()).setUnit(task.getUnit().name())
                .setParam(JSONObject.toJSONString(task.getParam())).setSource(ClientService.getConfig().getAddress())
                .setExecuter(broker).setSubmitModel(submitModel);
        return builder.build();
    }
}
