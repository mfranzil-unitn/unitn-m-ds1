package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.common.Log;
import it.unitn.ds1.common.LogLevel;
import it.unitn.ds1.project.message.DSSWelcomeMsg;
import it.unitn.ds1.project.message.dss.DSSMessage;
import it.unitn.ds1.project.message.dss.Recovery;
import it.unitn.ds1.project.message.dss.RequestSummaryMsg;
import it.unitn.ds1.project.message.dss.Timeout;
import it.unitn.ds1.project.message.dss.decision.DSSDecision;
import it.unitn.ds1.project.message.dss.decision.DSSDecisionRequest;
import it.unitn.ds1.project.message.dss.decision.DSSDecisionResponse;
import it.unitn.ds1.project.message.dss.read.DSSReadRequestMsg;
import it.unitn.ds1.project.message.dss.read.DSSReadResultMsg;
import it.unitn.ds1.project.message.dss.vote.DSSVote;
import it.unitn.ds1.project.message.dss.vote.DSSVoteRequest;
import it.unitn.ds1.project.message.dss.vote.DSSVoteResponse;
import it.unitn.ds1.project.message.dss.write.DSSWriteRequestMsg;
import it.unitn.ds1.project.model.DataItem;
import it.unitn.ds1.project.model.PrivateWorkspace;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/*-- The data store -----------------------------------------------------------*/
public class DSS extends AbstractNode {

    private final Map<Integer, DataItem> items = new HashMap<>();
    private final Map<String, PrivateWorkspace> privateWorkspaces = new HashMap<>();
    private final Map<String, List<DataItem>> lockedItems = new HashMap<>();

    private final Map<String, ActorRef> coordinators = new HashMap<>();
    private final List<ActorRef> dataStores = new ArrayList<>();
    private final Map<String, DSSVote> votes = new HashMap<>();

    private final BufferedWriter writeAheadLog;

    public DSS(int id, int lowerBound) {
        super(id);

        try {
            writeAheadLog = new BufferedWriter(new FileWriter(this.id + "-log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = lowerBound; i < lowerBound + 10; i++) {
            //   this.items.put(i, new DataItem(r.nextInt(), 1));
            this.items.put(i, new DataItem(100, 1));
        }
    }

    static public Props props(int id, int lowerBound) {
        return Props.create(DSS.class, () -> new DSS(id, lowerBound));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // GENERAL
                .match(DSSWelcomeMsg.class, this::onDSSWelcome)

                // COORDINATOR -> DSS
                .match(DSSReadRequestMsg.class, this::onDSSReadRequest)
                .match(DSSWriteRequestMsg.class, this::onDSSWriteRequest)

                .match(DSSVoteRequest.class, this::onVoteRequest)

                .match(DSSDecisionRequest.class, this::onDecisionRequest)
                .match(DSSDecisionResponse.class, this::onDecisionResponse)

                .match(Timeout.class, this::onTimeout)
                .match(Recovery.class, this::onRecovery)

                // DSS -> DSS
                .match(DSSVoteResponse.class, this::onVoteResponse)

                .match(RequestSummaryMsg.class, this::onRequestSummary)

                .build();
    }

    /*-- General messages ----------------------------------------------------- */

    private void onDSSWelcome(DSSWelcomeMsg msg) {
        this.dataStores.addAll(msg.dss);
    }

    /* -- R/W messages -------------------------- */

    private void onDSSReadRequest(DSSReadRequestMsg msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received DSSReadRequest for tID " + msg.transactionID + ", key: " + msg.key);
        PrivateWorkspace currentPrivateWorkspace = getWorkspace(msg.transactionID);

        if (!currentPrivateWorkspace.containsKey(msg.key)) {
            DataItem copy = new DataItem(this.items.get(msg.key));
            currentPrivateWorkspace.put(msg.key, copy);
        }

        DataItem copiedItem = currentPrivateWorkspace.get(msg.key);
        DSSReadResultMsg responseMsg = new DSSReadResultMsg(msg.transactionID, msg.key, copiedItem.getValue());

        ActorRef sender = getSender();
        delay(r.nextInt(MAX_DELAY));
        sender.tell(responseMsg, this.getSelf());
        Log.log(LogLevel.DEBUG, this.id, "Sent DSSReadResponse");
    }

    private void onDSSWriteRequest(DSSWriteRequestMsg msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received DSSWriteRequest for tID " + msg.transactionID + ", key: " + msg.key + ", value: " + msg.value);

        PrivateWorkspace currentPrivateWorkspace = getWorkspace(msg.transactionID);

        if (!currentPrivateWorkspace.containsKey(msg.key)) {
            DataItem copy = new DataItem(this.items.get(msg.key));
            currentPrivateWorkspace.put(msg.key, copy);
        }

        try {
            writeAheadLog.write(id + "@" + msg.transactionID + ";" + msg.key
                    + "=" + msg.value + ";v=" + currentPrivateWorkspace.get(msg.key).getVersion());
            writeAheadLog.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        currentPrivateWorkspace.get(msg.key).setValue(msg.value);
        currentPrivateWorkspace.get(msg.key).incrementVersion();

        // No message is sent for writes
        //this.getSender().tell(new DSSWriteResultMsg(msg.transactionID), getSelf());
    }

    private PrivateWorkspace getWorkspace(String transactionID) {
        // Add actorRef on coordinators if not present
        this.coordinators.putIfAbsent(transactionID, getSender());

        PrivateWorkspace currentPrivateWorkspace = this.privateWorkspaces.get(transactionID);

        if (currentPrivateWorkspace == null) {
            currentPrivateWorkspace = new PrivateWorkspace();
            this.privateWorkspaces.put(transactionID, currentPrivateWorkspace);
        }

        return currentPrivateWorkspace;
    }

    /* -- Commit messages ---------------------- */

    public void onVoteRequest(DSSVoteRequest msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received VoteRequest for tID " + msg.transactionID);
        if (!this.hasVoted(msg.transactionID)) {
            this.checkConsistency(msg);
        }

        if (Init.CRASH_DSS_BEFORE_VOTE_RESPONSE) {
            crash();
            return;
        }

        delay(r.nextInt(MAX_DELAY));
        this.getSender().tell(new DSSVoteResponse(msg.transactionID, votes.get(msg.transactionID)), getSelf());
        setTimeout(msg.transactionID, DECISION_TIMEOUT);

        if (Init.CRASH_DSS_BEFORE_DECISION_RESPONSE) {
            crash();
        }
    }

    private void onDecisionResponse(DSSDecisionResponse msg) {
        if (msg.decision == null) {
            throw new RuntimeException(id + ": received empty DSSDecisionResponse from " + msg.transactionID);
        }

        timeouts.get(msg.transactionID).cancel();
        PrivateWorkspace privateWorkspace = this.privateWorkspaces.get(msg.transactionID);

        if (privateWorkspace != null) {
            synchronized (System.out) {
                Log.partialLog(true, LogLevel.DEBUG, id, "Touched in tID " + msg.transactionID + ": [");
                privateWorkspace.forEach((k, v) -> {
                    Log.partialLog(false, LogLevel.DEBUG, id, "" + k + ", ");//+  ": " + v.getValue() + "], ");
                });
                Log.partialLog(false, LogLevel.DEBUG, id, "]\n");
            }

            switch (msg.decision) {
                case COMMIT:
                    privateWorkspace.forEach((key, value) -> {
                        this.items.get(key).setValue(value.getValue());
                        this.items.get(key).setVersion(value.getVersion());
                        this.items.get(key).releaseLock();
                    });
                    break;
                case ABORT:
                    List<DataItem> dataItems = this.lockedItems.get(msg.transactionID);
                    if (dataItems != null) {
                        dataItems.forEach(DataItem::releaseLock);
                    }
                    break;
            }

            this.privateWorkspaces.remove(msg.transactionID);
        } else {
            // Not my business, let's continue
        }
        this.coordinators.remove(msg.transactionID);
        fixDecision(msg.transactionID, msg.decision);
    }


    private void onVoteResponse(DSSVoteResponse msg) {
        if (msg.vote.equals(DSSVote.NO))
            multicast(new DSSDecisionResponse(msg.transactionID, DSSDecision.ABORT));
    }

    public void onRequestSummary(RequestSummaryMsg msg) {
        int sum = 0;
        for (DataItem d : items.values()) {
            sum += d.getValue();
        }

        Log.log(LogLevel.BASIC, this.id, "Sum: " + sum);
    }

    @Override
    protected void onTimeout(Timeout msg) {
        timeouts.remove(msg.transactionID);
        // we assume that vote request arrives sooner or later so no forced abort
        Log.log(LogLevel.BASIC, this.id, "Timeout. Decided? " + hasDecided(msg.transactionID)
                + ". My vote? " + votes.get(msg.transactionID));
        if (!hasDecided(msg.transactionID)) {
            if (votes.get(msg.transactionID) == DSSVote.YES) {
                //Log.log(this.id, "Timeout. Asking around.");
                multicast(new DSSVoteRequest(msg.transactionID));

                // If nobody responds to the timeout (e.g. if the coordinator crashed when everyone was in ready
                // state and someone received a voteRequest, then the transaction will be aborted as soon as
                // the next voteRequest arrives
            }
        }
    }

    @Override
    protected void onRecovery(Recovery msg) {
        getContext().become(createReceive());
        // We don't handle explicitly the "not voted" case here
        // (in any case, it does not break the protocol)
        for (String transactionID : privateWorkspaces.keySet()) {
            if (!hasDecided(transactionID)) {
                Log.log(LogLevel.BASIC, this.id, "Recovery. Asking the coordinator for tID + " + transactionID);
                coordinators.keySet().forEach(key -> {
                    ActorRef actor = coordinators.get(key);
                    delay(r.nextInt(MAX_DELAY));
                    actor.tell(new DSSDecisionRequest(transactionID), getSelf());
                    setTimeout(transactionID, DECISION_TIMEOUT);
                });
            }
        }
    }

    @Override
    protected void multicast(DSSMessage m) {
        for (ActorRef p : dataStores) {
            p.tell(m, getSelf());
        }
        coordinators.get(m.transactionID).tell(m, getSelf());
    }

    @Override
    protected void multicastAndCrash(DSSMessage m) {
        for (ActorRef p : dataStores) {
            p.tell(m, getSelf());
            crash();
            return;
        }
        // coordinators.get(m.transactionID).tell(m, getSelf());
    }

    private void checkConsistency(DSSMessage msg) {
        PrivateWorkspace currentPrivateWorkspace =
                this.privateWorkspaces.getOrDefault(msg.transactionID, new PrivateWorkspace());
        boolean commit = true;
        List<DataItem> locked = new ArrayList<>();

        for (Iterator<Map.Entry<Integer, DataItem>> iterator = currentPrivateWorkspace.entrySet().iterator();
             iterator.hasNext() && commit;
        ) {
            Map.Entry<Integer, DataItem> modifiedEntry = iterator.next();
            DataItem originalDataItem = this.items.get(modifiedEntry.getKey());

            if (originalDataItem.acquireLock()) {
                locked.add(originalDataItem);
            }

            if (!originalDataItem.getVersion().equals(modifiedEntry.getValue().getVersion() - 1) &&
                    !originalDataItem.getVersion().equals(modifiedEntry.getValue().getVersion())) {
                Log.log(LogLevel.BASIC, this.id, "Failed to acquire lock for item " + modifiedEntry.getKey());
                commit = false;
            }
        }

        if (!commit) {
            this.lockedItems.put(msg.transactionID, locked);
            this.getSelf().tell(new DSSDecisionResponse(msg.transactionID, DSSDecision.ABORT), getSelf());
            votes.put(msg.transactionID, DSSVote.NO);
            Log.log(LogLevel.BASIC, this.id, "Sending vote NO");
        } else {
            votes.put(msg.transactionID, DSSVote.YES);
            Log.log(LogLevel.INFO, this.id, "Sending vote YES");
        }
    }

    private boolean hasVoted(String tID) {
        return this.votes.get(tID) != null;

    }
}