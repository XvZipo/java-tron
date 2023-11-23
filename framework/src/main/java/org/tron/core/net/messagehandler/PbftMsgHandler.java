package org.tron.core.net.messagehandler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.Striped;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.consensus.base.Param;
import org.tron.consensus.pbft.PbftManager;
import org.tron.consensus.pbft.message.PbftBaseMessage;
import org.tron.consensus.pbft.message.PbftMessage;
import org.tron.core.config.args.Args;
import org.tron.core.exception.P2pException;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.TronNetService;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol;

@Component
@Slf4j(topic = "net")
public class PbftMsgHandler {

  private static final Striped<Lock> striped = Striped.lazyWeakLock(1024);

  private static final Cache<String, Boolean> msgCache = CacheBuilder.newBuilder()
      .initialCapacity(3000).maximumSize(10000).expireAfterWrite(10, TimeUnit.MINUTES).build();

  private static volatile Map<String, PbftMessage> msgGathered = new HashMap<>();
  private static volatile Map<String, PbftMessage> msgQueue = new HashMap<>();

  private static volatile long msgQueueMaxSize = Args.getInstance().pbftMsgMaxQueueSize;
  private static volatile long msgMaxSpeed = Args.getInstance().pbftMsgMaxSpeed;

  private static volatile long[] msgTotalSent = {0};
  private static volatile long[] msgSuccessSent = {0};
  private static volatile long[] tmpTimestamp = {0};
  static {
    new Thread(() -> {
      if(msgQueueMaxSize == 0 || msgMaxSpeed ==0){
        logger.info("@@@ msgQueueMaxSize or msgMaxSpeed was not found in config file. task failed..");
        return;
      }
      while (true) {
        try {
          logger.info("@@@ msgQueue size {}, peers {}", msgQueue.size(), TronNetService.getPeers().size());
          if (msgQueue.size() >= msgQueueMaxSize && TronNetService.getPeers().size() >= 1) {
            tmpTimestamp[0] = System.currentTimeMillis();
            msgQueue.values().forEach(v -> {
              try{
                msgTotalSent[0]++;
                if (msgTotalSent[0] % msgMaxSpeed == 0) {
                  long s = tmpTimestamp[0] + 1000 - System.currentTimeMillis();
                  logger.info("&&& total send {}, success count {}, sleep {}ms", msgTotalSent[0], msgSuccessSent[0], s);
                  if (s > 0) {
                    Thread.sleep(s);
                  }
                  tmpTimestamp[0] = System.currentTimeMillis();
                  msgSuccessSent[0] = 0;
                }
              }catch (Exception e){}
              List<PeerConnection> list = TronNetService.getPeers();
              if (list.size() == 0) {
                logger.info("@@@ peer size == 0 return...");
                return;
              }
              int index = new Random().nextInt(list.size());
              PeerConnection peerConnection = list.get(index);
              peerConnection.sendMessage(v);
              msgSuccessSent[0]++;
            });
          } else {
            Thread.sleep(1000);
          }
        }catch (Exception e) {
          logger.error("@@@ adv msg failed.", e);
          try{ Thread.sleep(1000); }catch (Exception e2){}
        }
      }
    }).start();
  }

  public static volatile long cnt = 0;
  @Autowired
  private PbftManager pbftManager;

  @Autowired
  private TronNetDelegate tronNetDelegate;

  public void processMessage(PeerConnection peer, PbftMessage msg) throws Exception {
    if (Param.getInstance().getPbftInterface().isSyncing()) {
      return;
    }
    if (msg.getDataType().equals(Protocol.PBFTMessage.DataType.BLOCK)
        && tronNetDelegate.getHeadBlockId().getNum() - msg.getNumber()
        > Args.getInstance().getPBFTExpireNum()) {
      return;
    }
    long currentEpoch = tronNetDelegate.getNextMaintenanceTime();
    long expireEpoch = 2 * tronNetDelegate.getMaintenanceTimeInterval();
    if (msg.getDataType().equals(Protocol.PBFTMessage.DataType.SRL)
        && currentEpoch - msg.getEpoch() > expireEpoch) {
      return;
    }
    msg.analyzeSignature();
    String key = buildKey(msg);
    Lock lock = striped.get(key);
    try {
      lock.lock();
      if (msgCache.getIfPresent(key) != null) {
        return;
      }
      if (!pbftManager.verifyMsg(msg)) {
        throw new P2pException(P2pException.TypeEnum.BAD_MESSAGE, msg.toString());
      }
      msgCache.put(key, true);
      if (msgQueueMaxSize>0 && msgMaxSpeed >0){
        msgGathered.put(key, msg);
        cnt++;
        logger.info("### cnt = {}, map1-size = {}, map2-size = {}",
            cnt, msgGathered.size(), msgQueue.size());
        if (msgGathered.size() >= msgQueueMaxSize) {
          msgQueue = msgGathered;
          msgGathered = new HashMap<>();
        }
      }
      forwardMessage(peer, msg);
      pbftManager.doAction(msg);
    } finally {
      lock.unlock();
    }
  }



  public void forwardMessage(PeerConnection peer, PbftBaseMessage message) {
    TronNetService.getPeers().stream().filter(peerConnection -> !peerConnection.equals(peer))
        .forEach(peerConnection -> peerConnection.sendMessage(message));
  }

  private String buildKey(PbftBaseMessage msg) {
    return msg.getKey() + msg.getPbftMessage().getRawData().getMsgType().toString();
  }

}