package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.common.BidiHashMap;
import it.unitn.ds1.common.Log;
import it.unitn.ds1.common.LogLevel;
import it.unitn.ds1.project.message.CoordinatorWelcomeMsg;
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
import it.unitn.ds1.project.message.txn.begin.TxnAcceptMsg;
import it.unitn.ds1.project.message.txn.begin.TxnBeginMsg;
import it.unitn.ds1.project.message.txn.end.TxnEndMsg;
import it.unitn.ds1.project.message.txn.end.TxnResultMsg;
import it.unitn.ds1.project.message.txn.read.TxnReadRequestMsg;
import it.unitn.ds1.project.message.txn.read.TxnReadResultMsg;
import it.unitn.ds1.project.message.txn.write.TxnWriteRequestMsg;

import java.util.*;

public class Coordinator extends AbstractNode {

    // list of datastore actors
    private final List<ActorRef> dataStores = new ArrayList<>();

    // maps actorRefs of clients to transactionID strings
    private final BidiHashMap<ActorRef, String> transactionMapping = new BidiHashMap<>();

    // here all the nodes that sent YES are collected
    private final HashMap<String, Set<ActorRef>> yesVotersMap = new HashMap<>();

    // list of DSS contacted for a transaction
    private final HashMap<String, Set<ActorRef>> dataStoreMapping = new HashMap<>();

    /*-- Actor constructor ---------------------------------------------------- */

    public Coordinator(int id) {
        super(id);
    }

    static public Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // GENERAL
                .match(CoordinatorWelcomeMsg.class, this::onCoordinatorWelcome)

                // CLIENT --> COORDINATOR
                .match(TxnBeginMsg.class, this::onTxnBegin)
                .match(TxnReadRequestMsg.class, this::onTxnReadRequest)
                .match(TxnWriteRequestMsg.class, this::onTxnWriteRequest)
                .match(TxnEndMsg.class, this::onTxnEnd)

                // COORDINATOR <- DSS
                .match(DSSReadResultMsg.class, this::onDSSReadResult)

                .match(DSSVoteResponse.class, this::onDSSVoteResponse)

                .match(DSSDecisionRequest.class, this::onDSSDecisionRequest)

                .match(Timeout.class, this::onTimeout)
                .match(Recovery.class, this::onRecovery)
                .build();
    }

    /*-- General messages ----------------------------------------------------- */

    private void onCoordinatorWelcome(CoordinatorWelcomeMsg msg) {
        this.dataStores.addAll(msg.dss);
    }

    /*-- Actor methods (for Client) -------------------------------------------------------- */

    private void onTxnBegin(TxnBeginMsg msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received TxnBegin from " + msg.clientId);
        String transactionID = UUID.randomUUID().toString();

        this.transactionMapping.put(getSender(), transactionID);
        this.yesVotersMap.putIfAbsent(transactionID, new HashSet<>());
        this.dataStoreMapping.putIfAbsent(transactionID, new HashSet<>());

        delay(r.nextInt(MAX_DELAY));
        getSender().tell(new TxnAcceptMsg(), getSelf());
        Log.log(LogLevel.INFO, this.id, "Assigned tID " + transactionID
                + " to Txn involving client" + msg.clientId);
    }

    private void onTxnReadRequest(TxnReadRequestMsg msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received TxnReadRequest from  "
                + msg.clientId + " and key " + msg.key);

        // Forwarding request to relevant DSS
        String transactionID = transactionMapping.get(getSender());
        ActorRef destination = getCorrespondingDSS(msg.key);

        this.dataStoreMapping.get(transactionID).add(destination);

        delay(r.nextInt(MAX_DELAY));
        destination.tell(new DSSReadRequestMsg(transactionID, msg.key), getSelf());
        // No response
    }

    private void onTxnWriteRequest(TxnWriteRequestMsg msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received TxnWriteRequest from "
                + msg.clientId + ", key: " + msg.key + ", value: " + msg.value);

        // Forwarding request to relevant DSS
        String transactionID = transactionMapping.get(getSender());
        ActorRef destination = getCorrespondingDSS(msg.key);

        this.dataStoreMapping.get(transactionID).add(destination);

        delay(r.nextInt(MAX_DELAY));
        destination.tell(new DSSWriteRequestMsg(transactionID, msg.key, msg.value), getSelf());

    }

    private void onTxnEnd(TxnEndMsg msg) {
        // on txnend the client is blocked until the coordinator has a ABORT/COMMIT decision
        // we need to start the logic for initiating the 2pc
        String transactionID = transactionMapping.get(getSender());

        // If the client chose not to commit we must respect his choice
        if (!msg.commit) {
            fixDecision(transactionID, DSSDecision.ABORT);

            multicast(new DSSDecisionResponse(transactionID, decision.get(transactionID)));

            ActorRef destination = transactionMapping.getKey(transactionID);
            delay(r.nextInt(MAX_DELAY));
            destination.tell(new TxnResultMsg(false), getSelf());
        } else {
            setTimeout(transactionID, VOTE_TIMEOUT);
            //crashyVoteRequest(transactionID);
            multicast(new DSSVoteRequest(transactionID));
        }
    }


    /*-- Actor methods (for DSS) -------------------------------------------------------- */

    private void onDSSReadResult(DSSReadResultMsg msg) {
        Log.log(LogLevel.DEBUG, this.id, "Received DSSReadResult for tID " + msg.transactionID
                + ": k=" + msg.key + ", v=" + msg.value);
        // Get who asked for the value originally
        ActorRef destination = transactionMapping.getKey(msg.transactionID);
        // Tell client of <key, value>
        delay(r.nextInt(MAX_DELAY));
        destination.tell(new TxnReadResultMsg(msg.key, msg.value), getSelf());
        Log.log(LogLevel.DEBUG, this.id, "Sent TxnReadResult");

    }

    /* -- 2PC methods (for DSS) ------------------ */

    @Override
    protected void onTimeout(Timeout msg) {
        timeouts.remove(msg.transactionID);
        if (!hasDecided(msg.transactionID)) {
            Log.log(LogLevel.BASIC, this.id, "Timeout. Decision not taken, I'll just abort.");
            fixDecision(msg.transactionID, DSSDecision.ABORT);

            // crashyDecisionResponse(msg.transactionID);
            multicast(new DSSDecisionResponse(msg.transactionID, decision.get(msg.transactionID)));

            // Inform client of sad decision
            ActorRef destination = transactionMapping.getKey(msg.transactionID);
            delay(r.nextInt(MAX_DELAY));
            destination.tell(new TxnResultMsg(false), getSelf());
        }
    }

    private void onDSSVoteResponse(DSSVoteResponse msg) {
        //log("Received DSSVoteResponse with content v = "
        //        + msg.vote + " total yes? " + yesVotersMap.get(msg.transactionID).size());

        if (hasDecided(msg.transactionID)) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }

        DSSVote v = msg.vote;
        Set<ActorRef> transactionVoters = yesVotersMap.get(msg.transactionID);

        if (v == DSSVote.YES) {
            transactionVoters.add(getSender());

            if (allVotedYes(msg.transactionID)) {
                Log.log(LogLevel.INFO, this.id, "Received all YES votes. Committing");
                timeouts.get(msg.transactionID).cancel();
                fixDecision(msg.transactionID, DSSDecision.COMMIT);
                yesVotersMap.remove(msg.transactionID);

                multicast(new DSSDecisionResponse(msg.transactionID, decision.get(msg.transactionID)));
                //crashyDecisionResponse(msg.transactionID);
                //return;
            } else {
                Log.log(LogLevel.INFO, this.id, "Received some YES votes. Keep going");
                return; // nothing to do, we need to wait some more
            }
        } else { // a NO vote
            // on a single NO we decide ABORT
            Log.log(LogLevel.INFO, this.id, "Received one NO vote. Aborting");
            fixDecision(msg.transactionID, DSSDecision.ABORT);
            multicast(new DSSDecisionResponse(msg.transactionID, decision.get(msg.transactionID)));
        }

        ActorRef originalSender = transactionMapping.getKey(msg.transactionID);
        delay(r.nextInt(MAX_DELAY));
        originalSender.tell(new TxnResultMsg(decision.get(msg.transactionID) == DSSDecision.COMMIT),
                getSelf());
    }

    private void crashyDecisionResponse(String transactionID) {
        if (Init.CRASH_COORDINATOR_AFTER_ONE_DECISION_RESPONSE && !Init.CRASH_COORDINATOR_AFTER_ALL_DECISION_RESPONSE) {
            multicastAndCrash(new DSSDecisionResponse(transactionID, decision.get(transactionID)));
        } else if (
                (!Init.CRASH_COORDINATOR_AFTER_ONE_DECISION_RESPONSE && Init.CRASH_COORDINATOR_AFTER_ALL_DECISION_RESPONSE) ||
                        (Init.CRASH_COORDINATOR_AFTER_ONE_DECISION_RESPONSE && Init.CRASH_COORDINATOR_AFTER_ALL_DECISION_RESPONSE)
        ) {
            multicast(new DSSDecisionResponse(transactionID, decision.get(transactionID)));
            crash();
        } else {
            multicast(new DSSDecisionResponse(transactionID, decision.get(transactionID)));
        }
    }

    private void crashyVoteRequest(String transactionID) {
        if (Init.CRASH_COORDINATOR_AFTER_ONE_VOTE_REQUEST && !Init.CRASH_COORDINATOR_AFTER_ALL_VOTE_REQUEST) {
            multicastAndCrash(new DSSVoteRequest(transactionID));
        } else if (
                (!Init.CRASH_COORDINATOR_AFTER_ONE_VOTE_REQUEST && Init.CRASH_COORDINATOR_AFTER_ALL_VOTE_REQUEST) ||
                        (Init.CRASH_COORDINATOR_AFTER_ONE_VOTE_REQUEST && Init.CRASH_COORDINATOR_AFTER_ALL_VOTE_REQUEST)
        ) {
            multicast(new DSSVoteRequest(transactionID));
            crash();
        } else {
            multicast(new DSSVoteRequest(transactionID));
        }
    }

    @Override
    protected void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        transactionMapping.forEach((client, transactionID) -> {
            Log.log(LogLevel.BASIC, this.id, "Recovery. Decided? " + hasDecided(transactionID)
                    + ". My decision? "
                    + (decision.get(transactionID) == null ? "null" : decision.get(transactionID)));

            if (!hasDecided(transactionID)) {
                fixDecision(transactionID, DSSDecision.ABORT);
            }

            // crashyDecisionResponse(msg.transactionID);
            multicast(new DSSDecisionResponse(transactionID, decision.get(transactionID)));

            client.tell(new TxnResultMsg(decision.get(transactionID) == DSSDecision.COMMIT), getSelf());
        });
    }


    @Override
    protected void multicast(DSSMessage m) {
        for (ActorRef datastore : dataStores) {
            datastore.tell(m, getSelf());
        }
    }

    @Override
    protected void multicastAndCrash(DSSMessage m) {
        // Crashes after one message
        for (ActorRef datastore : dataStores) {
            datastore.tell(m, getSelf());
            break;
        }
        crash();
    }

    /* -- Auxiliary ------------------------ */

    private boolean allVotedYes(String transactionID) {
        Set<ActorRef> contacted = this.dataStoreMapping.get(transactionID);
        Set<ActorRef> voters = yesVotersMap.get(transactionID);
        return voters.size() >= contacted.size();
    }

    private ActorRef getCorrespondingDSS(int key) {
        return dataStores.get((key - (key % 10)) / 10);
    }

}