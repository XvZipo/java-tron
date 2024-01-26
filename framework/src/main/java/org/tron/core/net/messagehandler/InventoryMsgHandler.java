package org.tron.core.net.messagehandler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.config.args.Args;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.TronNetService;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.adv.InventoryMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.adv.AdvService;
import org.tron.protos.Protocol.Inventory.InventoryType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j(topic = "net")
@Component
public class InventoryMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private AdvService advService;

  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;





  private static volatile Map<Integer, TronMessage> msgGathered = new HashMap<>();
  private static volatile Map<Integer, TronMessage> msgQueue = new HashMap<>();

  private static volatile long msgQueueMaxSize = Args.getInstance().inventoryMsgMaxQueueSize;
  private static volatile long msgMaxSpeed = Args.getInstance().inventoryMsgMaxSpeed;

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


  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) {
    InventoryMessage inventoryMessage = (InventoryMessage) msg;
    if (msgQueueMaxSize>0 && msgMaxSpeed >0){
      msgGathered.put(msg.hashCode(), msg);
      cnt++;
      logger.info("### cnt = {}, map1-size = {}, map2-size = {}",
          cnt, msgGathered.size(), msgQueue.size());
      if (msgGathered.size() >= msgQueueMaxSize) {
        msgQueue = msgGathered;
        msgGathered = new HashMap<>();
      }
    }


    InventoryType type = inventoryMessage.getInventoryType();

    if (!check(peer, inventoryMessage)) {
      logger.info("inventoryMessage check fail!");
      return;
    }

    for (Sha256Hash id : inventoryMessage.getHashList()) {
      Item item = new Item(id, type);
      peer.getAdvInvReceive().put(item, System.currentTimeMillis());
      advService.addInv(item);
    }
  }

  private boolean check(PeerConnection peer, InventoryMessage inventoryMessage) {

    InventoryType type = inventoryMessage.getInventoryType();
    int size = inventoryMessage.getHashList().size();

    if (peer.isNeedSyncFromPeer() || peer.isNeedSyncFromUs()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, syncFromUs: {}, syncFromPeer: {}",
          type, size, peer.getInetAddress(), peer.isNeedSyncFromUs(), peer.isNeedSyncFromPeer());
      return false;
    }

    if (type.equals(InventoryType.TRX) && tronNetDelegate.isBlockUnsolidified()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, block unsolidified",
          type, size, peer.getInetAddress());
      return false;
    }

    if (type.equals(InventoryType.TRX) && transactionsMsgHandler.isBusy()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, transactionsMsgHandler is busy",
              type, size, peer.getInetAddress());
      if (Args.getInstance().isOpenPrintLog()) {
        logger.warn("[isBusy]Drop tx list is: {}", inventoryMessage.getHashList());
      }
      return false;
    }

    return true;
  }
}
