package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.project.message.DSSWelcomeMsg;
import it.unitn.ds1.project.message.dss.DSSMessage;
import it.unitn.ds1.project.message.dss.Recovery;
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

import java.util.*;

/*-- The data store -----------------------------------------------------------*/
public class DSS extends AbstractNode {

    private final Map<Integer, DataItem> items = new HashMap<>();
    private final Map<String, PrivateWorkspace> privateWorkspaces = new HashMap<>();
    private final Map<String, List<DataItem>> lockedItems = new HashMap<>();

    private Map<String, ActorRef> coordinators = new HashMap<>();
    private final List<ActorRef> dataStores = new ArrayList<>();

    private DSSVote vote;

    private final Random r;

    public DSS(int id, int lowerBound) {
        super(id);
        this.vote = null;
        this.r = new Random();
        for (int i = lowerBound; i < lowerBound + 10; i++) {
            this.items.put(i, new DataItem(r.nextInt(), 1));
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
                .match(DSSVoteRequest.class, this::onVoteRequest)

                .match(DSSDecisionRequest.class, this::onDecisionRequest)
                .match(DSSDecisionResponse.class, this::onDecisionResponse)

                .match(Timeout.class, this::onTimeout)
                .match(Recovery.class, this::onRecovery)
                .match(DSSReadRequestMsg.class, this::onDSSReadRequest)
                .match(DSSWriteRequestMsg.class, this::onDSSWriteRequest)
                .build();
    }

    /*-- General messages ----------------------------------------------------- */

    private void onDSSWelcome(DSSWelcomeMsg msg) {
        this.dataStores.addAll(msg.dss);
    }

    /* -- R/W messages -------------------------- */

    private void onDSSReadRequest(DSSReadRequestMsg msg) {
        PrivateWorkspace currentPrivateWorkspace = getWorkspace(msg.transactionID);

        if (!currentPrivateWorkspace.containsKey(msg.key)) {
            DataItem copy = new DataItem(this.items.get(msg.key));
            currentPrivateWorkspace.put(msg.key, copy);
        }

        DataItem copiedItem = currentPrivateWorkspace.get(msg.key);
        DSSReadResultMsg responseMsg = new DSSReadResultMsg(msg.transactionID, msg.key, copiedItem.getValue());

        ActorRef sender = getSender();
        sender.tell(responseMsg, this.getSelf());
    }

    private void onDSSWriteRequest(DSSWriteRequestMsg msg) {
        PrivateWorkspace currentPrivateWorkspace = getWorkspace(msg.transactionID);

        if (!currentPrivateWorkspace.containsKey(msg.key)) {
            DataItem copy = new DataItem(this.items.get(msg.key));
            currentPrivateWorkspace.put(msg.key, copy);
        }

        currentPrivateWorkspace.get(msg.key).setValue(msg.value);

        // No message is sent for writes
        //this.getSender().tell(new DSSWriteResultMsg(msg.transactionID), getSelf());
    }

    private PrivateWorkspace getWorkspace(String transactionID) {
        // Add actorRef on coordinators if not present
        this.coordinators.putIfAbsent(transactionID, getSender());

        PrivateWorkspace currentPrivateWorkspace = this.privateWorkspaces.get(transactionID);

        if (currentPrivateWorkspace == null) {
            currentPrivateWorkspace = new PrivateWorkspace();
        }

        return currentPrivateWorkspace;
    }

    /* -- Commit messages ---------------------- */

    public void onVoteRequest(DSSVoteRequest msg) {
        DSSVote v;

        PrivateWorkspace currentPrivateWorkspace =
                this.privateWorkspaces.getOrDefault(msg.transactionID, new PrivateWorkspace());

        boolean commit = true;
        List<DataItem> locked = new ArrayList<>();

        for (Map.Entry<Integer, DataItem> modifiedEntry : currentPrivateWorkspace.entrySet()) {
            DataItem originalDataItem = this.items.get(modifiedEntry.getKey());
            if (originalDataItem.acquireLock()
                    && originalDataItem.getVersion() == modifiedEntry.getValue().getVersion() - 1) {
                locked.add(originalDataItem);
            } else {
                commit = false;
            }
        }
        if (!commit) {
            this.lockedItems.put(msg.transactionID, locked);
            this.getSelf().tell(new DSSDecisionResponse(msg.transactionID, DSSDecision.ABORT), getSelf());
            v = DSSVote.NO;
        } else {
            v = DSSVote.YES;
        }

        log("sending vote " + vote);
        this.getSender().tell(new DSSVoteResponse(msg.transactionID, v), getSelf());
        setTimeout(msg.transactionID, DECISION_TIMEOUT);

        /*if (id == 0) {
            crash(10000);
            return;
        }    // simulate a crash
        if (id == 1) {
            crash(10000);
            return;
        }    // simulate a crash
        if (id == 2) delay(4000);              // simulate a delay
        */
        /*
        vote = r.nextDouble() < COMMIT_PROBABILITY ? DSSVote.YES : DSSVote.NO;
        if (vote == DSSVote.NO) {
            fixDecision(DSSDecision.ABORT);
        }
        */
    }

    private void onDecisionResponse(DSSDecisionResponse msg) {
        if (msg.decision != null) {
            switch (msg.decision) {
                case COMMIT:
                    this.privateWorkspaces.get(msg.transactionID).forEach((key, value) -> {
                        this.items.get(key).setValue(value.getValue());
                        this.items.get(key).setVersion(value.getVersion());
                    });
                case ABORT:
                    this.coordinators.remove(msg.transactionID);
                    this.lockedItems.getOrDefault(msg.transactionID, new ArrayList<>())
                            .forEach(DataItem::releaseLock);
                    this.lockedItems.remove(msg.transactionID);
                    this.privateWorkspaces.remove(msg.transactionID);

            }
        }
        fixDecision(msg.transactionID, msg.decision);
    }

    @Override
    protected void onTimeout(Timeout msg) {
        // we assume that vote request arrives sooner or later so no forced abort
        log("---------" + hasDecided(msg.transactionID) + " - " + vote);
        if (!hasDecided(msg.transactionID)) {
            if (vote == DSSVote.YES) {
                log("Timeout. Asking around.");
                multicast(new DSSDecisionRequest(msg.transactionID));
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
                log("Recovery. Asking the coordinator.");
                coordinators.keySet().forEach(key -> {
                    ActorRef actor = coordinators.get(key);
                    actor.tell(new DSSDecisionRequest(transactionID), getSelf());
                    setTimeout(transactionID, DECISION_TIMEOUT);
                });
            }
        }
    }

    @Override
    protected void crash(int recoverIn) {
        // TODO make method for crashing
    }

    @Override
    protected void multicast(DSSMessage m) {
        for (ActorRef p : dataStores) {
            p.tell(m, getSelf());
        }
        coordinators.get(m.transactionID).tell(m, getSelf());
    }

    @Override
    protected void multicastAndCrash(DSSMessage m, int recoverIn) {
        for (ActorRef p : dataStores) {
            p.tell(m, getSelf());
            crash(recoverIn);
            return;
        }
        // coordinators.get(m.transactionID).tell(m, getSelf());
    }
}