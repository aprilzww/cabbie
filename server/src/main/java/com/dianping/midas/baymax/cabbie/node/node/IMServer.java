/*
 *Copyright 2014 DDPush
 *Author: AndyKwok(in English) GuoZhengzhu(in Chinese)
 *Email: ddpush@126.com
 *

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package com.dianping.midas.baymax.cabbie.node.node;


import com.dianping.midas.baymax.cabbie.node.node.pushlistener.NIOPushListener;
import com.dianping.midas.baymax.cabbie.node.node.tcpconnector.NIOTcpConnector;
import com.dianping.midas.baymax.cabbie.node.node.udpconnector.UdpConnector;
import com.dianping.midas.baymax.cabbie.node.node.weixinconnector.WeixinSender;
import com.dianping.midas.baymax.cabbie.node.utils.DateTimeUtil;
import com.dianping.midas.baymax.cabbie.node.utils.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;

public class IMServer {

    private static final Logger logger = LoggerFactory.getLogger(IMServer.class);

    public static IMServer server;

    private boolean stoped = false;

    //fixed work threads
    int workerNum = PropertyUtil.getPropertyInt("CLIENT_UDP_WORKER_THREAD");

    private UdpConnector udpConnector;

    private Thread weixinSenderThread;
    private WeixinSender weixinSender;

    private Thread tcpConnThread;
    private NIOTcpConnector tcpConnector;

    private NodeStatus nodeStatus = NodeStatus.getInstance();

    private ArrayList<Messenger> workerList = new ArrayList<Messenger>();

    private Thread cleanerThread = null;
    private ClientStatMachineCleaner cleaner = null;

    private Thread cmdThread = null;
    private IMServerConsole console = null;

    private Thread pushThread = null;
    private NIOPushListener pushListener = null;

    private long startTime;

    private IMServer() {

    }

    public static IMServer getInstance() {
        if (server == null) {
            synchronized (IMServer.class) {
                if (server == null) {
                    server = new IMServer();
                }
            }
        }
        return server;
    }

    public void init() throws Exception {
        initPushListener();
//        initConsole();
        initWeixinSender();
        initUdpConnector();
//        initTcpConnector();
//        initWorkers();
        initCleaner();
    }

    public void initConsole() throws Exception {
        console = new IMServerConsole();
        cmdThread = new Thread(console, "IMServer-console");
        cmdThread.setDaemon(true);
        cmdThread.start();
    }

    public void initUdpConnector() throws Exception {
        logger.info("start connector...");
        udpConnector = new UdpConnector();
        udpConnector.start();
    }

    public void initWeixinSender(){
        weixinSender = new WeixinSender();
        weixinSender.init();
        weixinSenderThread = new Thread(weixinSender,"IMServer-weixinSender");
        weixinSenderThread.start();
    }

    public void initTcpConnector() throws Exception {
        if (!"YES".equalsIgnoreCase(PropertyUtil.getProperty("TCP_CONNECTOR_ENABLE"))) {
            return;
        }
        tcpConnector = new NIOTcpConnector();
        tcpConnThread = new Thread(tcpConnector, "IMServer-NIOTcpConnector");
        synchronized (tcpConnector) {
            tcpConnThread.start();
            tcpConnector.wait();
        }
    }

    public void initWorkers() {
        System.out.println("start " + workerNum + " workers...");
        for (int i = 0; i < workerNum; i++) {
            Messenger worker = new Messenger(udpConnector, nodeStatus);
            workerList.add(worker);
            Thread t = new Thread(worker, "IMServer-worker-" + i);
            worker.setHostThread(t);
            t.setDaemon(true);
            t.start();
        }
    }

    public void initCleaner() throws Exception {
        cleaner = new ClientStatMachineCleaner();
        cleanerThread = new Thread(cleaner, "IMServer-cleaner");
        cleanerThread.start();
    }

    public void initPushListener() throws Exception {
        pushListener = new NIOPushListener();
        pushThread = new Thread(pushListener, "IMServer-push-listener");
        pushThread.start();
    }

    public void start() throws Exception {
        logger.info("working dir: " + System.getProperty("user.dir"));
        init();

        final Thread mainT = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                stoped = true;
                logger.info("shut down server... ");
                try {
                    mainT.join();
                    logger.info("server is down, bye ");
                } catch (Exception e) {
                    logger.error("stop with error!",e);
                }
            }
        });
        this.startTime = System.currentTimeMillis();
        logger.info("server is up ");
        while (stoped == false) {
            try {
                synchronized (this) {
                    this.wait(1000 * 60);
                    if (stoped == false) {
                        autoClean();
                    }
                }
            } catch (Exception e) {
                logger.error("clean mem error!",e);
            }
        }
        this.quit();

    }

    private void autoClean() {
        float percent = PropertyUtil.getPropertyFloat("CLEANER_AUTO_RUN_MEM_PERCENT");
        if (percent >= 1 || percent <= 0) {
            logger.error("property format error...");
            return;
        }
        Runtime rt = Runtime.getRuntime();
        if ((rt.totalMemory() - rt.freeMemory()) / (double) rt.maxMemory() > percent) {
            logger.info("run auto clean...");
            cleaner.wakeup();
        }
    }

    public void stop() {
        this.stoped = true;
        synchronized (this) {
            this.notifyAll();
        }

    }

    protected void quit() throws Exception {
        try {
//            stopWorkers();
            stopWeixinSender();
            stopUdpConnector();
//            stopTcpConnector();
            stopCleaner();
            stopPushListener();
        } catch (Throwable t) {
            logger.error("stop server error!",t);
        }
        saveStatus();
    }

    public void stopWorkers() throws Exception {
        for (int i = 0; i < workerList.size(); i++) {
            try {
                workerList.get(i).stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void stopUdpConnector() throws Exception {
        if (udpConnector == null) {
            return;
        }
        udpConnector.stop();
    }

    public void stopWeixinSender() throws Exception{
        if(weixinSender==null || weixinSenderThread==null){
            return;
        }
        weixinSender.stop();
        weixinSenderThread.join();
    }

    public void stopTcpConnector() throws Exception {
        if (tcpConnector == null || tcpConnThread == null) {
            return;
        }
        tcpConnector.stop();
        tcpConnThread.join();
    }

    public void stopCleaner() throws Exception {
        cleaner.stop();
        try {
            cleanerThread.interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stopPushListener() throws Exception {
        pushListener.stop();
        pushThread.join();
    }

    public void saveStatus() throws Exception {
        nodeStatus.saveToFile();
    }

    public String getStatusString() {
        StringBuffer sb = new StringBuffer();

        String end = "\r\n";

        sb.append("server start up at: ").append(DateTimeUtil.formatDate(new Date(this.startTime))).append(end);
        long runtime = System.currentTimeMillis() - this.startTime;
        sb.append("up time: ").append(runtime / (1000 * 3600 * 24)).append(" day ").append(runtime / (1000 * 3600)).append(" hour ").append(runtime / (1000 * 60)).append(" minute").append(end);
        sb.append("messagers: ").append(this.workerList.size()).append(end);
        sb.append("current stat machines: ").append(nodeStatus.size()).append(end);
        sb.append("udp recieve packages: ").append(this.udpConnector.getInqueueIn()).append(end);
        sb.append("udp recieve packages pending: ").append(this.udpConnector.getInqueueIn() - this.udpConnector.getInqueueOut()).append(end);
        sb.append("udp send packages: ").append(this.udpConnector.getOutqueueIn()).append(end);
        sb.append("udp send packages pending: ").append(this.udpConnector.getOutqueueIn() - this.udpConnector.getOutqueueOut()).append(end);
        sb.append("jvm  max  mem: ").append(Runtime.getRuntime().maxMemory()).append(end);
        sb.append("jvm total mem: ").append(Runtime.getRuntime().totalMemory()).append(end);
        sb.append("jvm  free mem: ").append(Runtime.getRuntime().freeMemory()).append(end);
        sb.append("last clean time: ").append(DateTimeUtil.formatDate(new Date(this.cleaner.getLastCleanTime()))).append(end);
        sb.append("messengers threads:----------------------").append(end);
        for (int i = 0; i < workerList.size(); i++) {
            Thread t = workerList.get(i).getHostThread();
            sb.append(t.getName() + " stat: " + t.getState().toString()).append(end);
        }
        return sb.toString();
    }

    public String getUuidStatString(String uuid) {
        ClientStatMachine csm = this.nodeStatus.getClientStat(uuid);
        if (csm == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        String end = "\r\n";

        sb.append("stat of   uuid: " + uuid).append(end);
        sb.append("last tick time: " + DateTimeUtil.formatDate(new Date(csm.getLastTick()))).append(end);
        sb.append("last ip addres: " + csm.getLastAddr()).append(end);
        sb.append("last tcp  time: " + DateTimeUtil.formatDate(new Date(csm.getMessengerTask() == null ? 0 : csm.getMessengerTask().getLastActive()))).append(end);
        sb.append("0x10   message: " + csm.has0x10Message()).append(end);
        sb.append("last 0x10 time: " + DateTimeUtil.formatDate(new Date(csm.getLast0x10Time()))).append(end);
        sb.append("0x11   message: " + csm.get0x11Message()).append(end);
        sb.append("last 0x11 time: " + DateTimeUtil.formatDate(new Date(csm.getLast0x11Time()))).append(end);
        sb.append("0x20   message: " + csm.has0x20Message()).append(end);
        sb.append("last 0x20 time: " + DateTimeUtil.formatDate(new Date(csm.getLast0x20Time()))).append(end);
        sb.append("0x20 arry  len: " + csm.getMessage0x20Len()).append(end);

        return sb.toString();
    }

    public void cleanExpiredMachines(int hours) {
        cleaner.setExpiredHours(hours);
        cleaner.wakeup();
    }

    public void pushInstanceMessage(ServerMessage sm) throws Exception {
        if (sm == null || sm.getData() == null || sm.getSocketAddress() == null) {
            return;
        }
        if (udpConnector != null) {
            udpConnector.send(sm);
        }
    }

    public void pushToWeixin(ServerMessage sm){
        if (sm == null || sm.getData() == null || sm.getSocketAddress() == null) {
            return;
        }
        if (weixinSender != null) {
            weixinSender.enqueue(sm);
        }
    }

    public static void main(String[] args) {
        IMServer server = IMServer.getInstance();
        try {
            server.start();

        } catch (Exception e) {
            logger.error("start server faild", e);
            System.exit(1);
        } catch (Throwable t) {
            logger.error("start server faild", t);
            System.exit(1);
        }
    }
}
