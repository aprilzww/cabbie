package com.dianping.midas.baymax.cabbie.node.node.weixinconnector;

import com.dianping.midas.baymax.cabbie.node.node.ServerMessage;
import com.dianping.midas.baymax.cabbie.node.utils.DateTimeUtil;
import com.dianping.midas.baymax.cabbie.node.utils.PropertyUtil;
import com.dianping.midas.baymax.cabbie.node.utils.StringUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: zww
 * Date: 16-3-15
 * Time: 13:32
 * To change this template use File | Settings | File Templates.
 */
public class WeixinSender implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WeixinSender.class);
    protected boolean stoped = false;
    protected Object enQueSignal = new Object();
    protected ByteBuffer bufferBody;
    protected AtomicLong queueIn = new AtomicLong(0);
    protected AtomicLong queueOut = new AtomicLong(0);
    protected int bufferBodySize =  PropertyUtil.getPropertyInt("PUSH_MSG_MAX_CONTENT_LEN");

    protected ConcurrentLinkedQueue<ServerMessage> mq = new ConcurrentLinkedQueue<ServerMessage>();

    public boolean enqueue(ServerMessage sm){
        boolean result = mq.add(sm);
        if (result == true) {
            queueIn.addAndGet(1);
        }
        return result;
    }

    public void stop() {
        this.stoped = true;
    }

    public void init() {
        bufferBody = ByteBuffer.allocate(bufferBodySize);
    }

    @Override
    public void run(){
        while (!this.stoped) {
            try {
                synchronized (enQueSignal) {
                    while (mq.isEmpty() == true && stoped == false) {
                        try {
                            enQueSignal.wait(1);
                        } catch (InterruptedException e) {

                        }
//                        System.out.println("sender wake up");
                    }
                    processMessage();

                }
            } catch (Exception e) {
                logger.error("sender thread unexpect error!",e);
            } catch (Throwable t) {
                logger.error("sender thread throwble error!",t);
            }
        }
    }

    /**
     * 发送消息，从队列中取出消息
     * @throws Exception
     */
    protected void processMessage() throws Exception {

        bufferBody.clear();
        ServerMessage pendingMessage = dequeue();
        if (pendingMessage == null) {
            //Thread.yield();
            return;
        }
        int length = (int) ByteBuffer.wrap(pendingMessage.getData(), 19, 2).getChar();
        if(length > bufferBodySize){
            logger.error("message oversize");
            length = bufferBodySize;
        }
        bufferBody = ByteBuffer.wrap(pendingMessage.getData(), 21, length);
//        bufferBody.flip();
        String message = Charset.forName("UTF-8").decode(bufferBody).toString();
        String[] pairs = message.split("&");
        String user = "";
        String test = "";
        for(String kv:pairs){
            String[] param = kv.split("=");
            if("userName".equalsIgnoreCase(param[0])){
                user = param[1];
            }else if("text".equalsIgnoreCase(param[0])){
                test = param[1];
            }
        }
        httpPost(user,test);
        logger.info(DateTimeUtil.getCurDateTime() + " s:" + StringUtil.convert(pendingMessage.getData()) + " to  weixin:"+ user);
    }

    protected ServerMessage dequeue() {
        ServerMessage m = mq.poll();
        if (m != null) {
            queueOut.addAndGet(1);
        }
        return m;
    }

    private void httpPost(String user,String test){

        HttpClient client = HttpClients.createDefault();
        HttpGet request = new HttpGet("http://ops-auth01.nh/api/sendWeixin?users="+ user +"&text=" + test);
        try {
            HttpResponse response = client.execute(request);
            int returnCode = response.getStatusLine().getStatusCode();
            if(returnCode!=200){
                //TODO:失败重试 enqueue
            }
        } catch (IOException e) {
            logger.error("http faild",e);
        }

    }

}
