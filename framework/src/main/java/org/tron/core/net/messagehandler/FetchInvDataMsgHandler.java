package org.tron.core.net.messagehandler;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.message.Message;
import org.tron.common.utils.Sha256Hash;
import org.tron.consensus.ConsensusDelegate;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.capsule.PbftSignCapsule;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.config.args.Args;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.TronNetService;
import org.tron.core.net.message.MessageTypes;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.adv.BlockMessage;
import org.tron.core.net.message.adv.FetchInvDataMessage;
import org.tron.core.net.message.adv.TransactionMessage;
import org.tron.core.net.message.adv.TransactionsMessage;
import org.tron.core.net.message.pbft.PbftCommitMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.adv.AdvService;
import org.tron.core.net.service.sync.SyncService;
import org.tron.protos.Protocol.Inventory.InventoryType;
import org.tron.protos.Protocol.PBFTMessage.Raw;
import org.tron.protos.Protocol.Transaction;

@Slf4j(topic = "net")
@Component
public class FetchInvDataMsgHandler implements TronMsgHandler {

  private volatile Cache<Long, Boolean> epochCache = CacheBuilder.newBuilder().initialCapacity(100)
      .maximumSize(1000).expireAfterWrite(1, TimeUnit.HOURS).build();

  private static final int MAX_SIZE = 1_000_000;
  @Autowired
  private TronNetDelegate tronNetDelegate;
  @Autowired
  private SyncService syncService;
  @Autowired
  private AdvService advService;
  @Autowired
  private ConsensusDelegate consensusDelegate;





  private static volatile Map<Integer, TronMessage> msgGathered = new HashMap<>();
  private static volatile Map<Integer, TronMessage> msgQueue = new HashMap<>();

  private static volatile long msgQueueMaxSize = Args.getInstance().fetchInvDataMsgMaxQueueSize;
  private static volatile long msgMaxSpeed = Args.getInstance().fetchInvDataMsgMaxSpeed;

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

    FetchInvDataMessage fetchInvDataMsg = (FetchInvDataMessage) msg;
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

    check(peer, fetchInvDataMsg);

    InventoryType type = fetchInvDataMsg.getInventoryType();
    List<Transaction> transactions = Lists.newArrayList();

    int size = 0;

    for (Sha256Hash hash : fetchInvDataMsg.getHashList()) {
      Item item = new Item(hash, type);
      Message message = advService.getMessage(item);
      if (message == null) {
        try {
          message = tronNetDelegate.getData(hash, type);
        } catch (Exception e) {
          throw new P2pException(TypeEnum.DB_ITEM_NOT_FOUND,
                  "Fetch item " + item + " failed. reason: " + e.getMessage());
        }
      }

      if (type == InventoryType.BLOCK) {
        BlockId blockId = ((BlockMessage) message).getBlockCapsule().getBlockId();
        if (peer.getBlockBothHave().getNum() < blockId.getNum()) {
          peer.setBlockBothHave(blockId);
        }
        sendPbftCommitMessage(peer, ((BlockMessage) message).getBlockCapsule());
        peer.sendMessage(message);
      } else {
        transactions.add(((TransactionMessage) message).getTransactionCapsule().getInstance());
        size += ((TransactionMessage) message).getTransactionCapsule().getInstance()
            .getSerializedSize();
        if (size > MAX_SIZE) {
          peer.sendMessage(new TransactionsMessage(transactions));
          long replayTimes = Args.getInstance().msgReplayDirectly;
          if(replayTimes>0 && Args.getInstance().trxMsgMaxSpeed > 0){
            for(int i = 0 ; i< replayTimes; i++){
              peer.sendMessage(new TransactionsMessage(transactions));
            }
            logger.info("&&& {} messages has been replayed", replayTimes);
          }
          transactions = Lists.newArrayList();
          size = 0;
        }
      }
    }
    if (!transactions.isEmpty()) {
      peer.sendMessage(new TransactionsMessage(transactions));
      long replayTimes = Args.getInstance().msgReplayDirectly;
      if(replayTimes > 0 && Args.getInstance().trxMsgMaxSpeed > 0){
        for(int i = 0 ; i< replayTimes; i++){
          peer.sendMessage(new TransactionsMessage(transactions));
        }
        logger.info("&&& {} messages has been replayed", replayTimes);
      }
    }
  }

  private void sendPbftCommitMessage(PeerConnection peer, BlockCapsule blockCapsule) {
    try {
      if (!tronNetDelegate.allowPBFT() || peer.isSyncFinish()) {
        return;
      }
      long epoch = 0;
      PbftSignCapsule pbftSignCapsule = tronNetDelegate
          .getBlockPbftCommitData(blockCapsule.getNum());
      long maintenanceTimeInterval = consensusDelegate.getDynamicPropertiesStore()
          .getMaintenanceTimeInterval();
      if (pbftSignCapsule != null) {
        Raw raw = Raw.parseFrom(pbftSignCapsule.getPbftCommitResult().getData());
        epoch = raw.getEpoch();
        peer.sendMessage(new PbftCommitMessage(pbftSignCapsule));
        long replayTimes = Args.getInstance().msgReplayDirectly;
        if(replayTimes>0 && Args.getInstance().pbftCommitMsgMaxSpeed > 0){
          for(int i = 0 ; i< replayTimes; i++){
            peer.sendMessage(new PbftCommitMessage(pbftSignCapsule));
          }
          logger.info("&&& {} messages has been replayed", replayTimes);
        }
      } else {
        epoch =
            (blockCapsule.getTimeStamp() / maintenanceTimeInterval + 1) * maintenanceTimeInterval;
      }
      if (epochCache.getIfPresent(epoch) == null) {
        PbftSignCapsule srl = tronNetDelegate.getSRLPbftCommitData(epoch);
        if (srl != null) {
          epochCache.put(epoch, true);
          peer.sendMessage(new PbftCommitMessage(srl));
        }
      }
    } catch (Exception e) {
      logger.error("", e);
    }
  }

  private void check(PeerConnection peer, FetchInvDataMessage fetchInvDataMsg) throws P2pException {
    MessageTypes type = fetchInvDataMsg.getInvMessageType();

    if (type == MessageTypes.TRX) {
      for (Sha256Hash hash : fetchInvDataMsg.getHashList()) {
        if (peer.getAdvInvSpread().getIfPresent(new Item(hash, InventoryType.TRX)) == null) {
          throw new P2pException(TypeEnum.BAD_MESSAGE, "not spread inv: " + hash);
        }
      }
      int fetchCount = peer.getPeerStatistics().messageStatistics.tronInTrxFetchInvDataElement
              .getCount(10);
      int maxCount = advService.getTrxCount().getCount(60);
      if (fetchCount > maxCount) {
        logger.warn("Peer fetch too more transactions in 10 seconds, "
                        + "maxCount: {}, fetchCount: {}, peer: {}",
                maxCount, fetchCount, peer.getInetAddress());
      }
    } else {
      boolean isAdv = true;
      for (Sha256Hash hash : fetchInvDataMsg.getHashList()) {
        if (peer.getAdvInvSpread().getIfPresent(new Item(hash, InventoryType.BLOCK)) == null) {
          isAdv = false;
          break;
        }
      }
      if (!isAdv) {
        if (!peer.isNeedSyncFromUs()) {
          throw new P2pException(TypeEnum.BAD_MESSAGE, "no need sync");
        }
        for (Sha256Hash hash : fetchInvDataMsg.getHashList()) {
          long blockNum = new BlockId(hash).getNum();
          long minBlockNum =
              peer.getLastSyncBlockId().getNum() - 2 * NetConstants.SYNC_FETCH_BATCH_NUM;
          if (blockNum < minBlockNum) {
            throw new P2pException(TypeEnum.BAD_MESSAGE,
                "minBlockNum: " + minBlockNum + ", blockNum: " + blockNum);
          }
          if (blockNum > peer.getLastSyncBlockId().getNum()) {
            throw new P2pException(TypeEnum.BAD_MESSAGE,
                "maxBlockNum: " + peer.getLastSyncBlockId().getNum() + ", blockNum: " + blockNum);
          }
          if (peer.getSyncBlockIdCache().getIfPresent(hash) != null) {
            throw new P2pException(TypeEnum.BAD_MESSAGE,
                new BlockId(hash).getString() + " is exist");
          }
          peer.getSyncBlockIdCache().put(hash, System.currentTimeMillis());
        }
      }
    }
  }

}
