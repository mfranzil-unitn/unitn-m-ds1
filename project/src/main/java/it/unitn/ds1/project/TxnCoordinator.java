package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.project.message.dss.Recovery;
import it.unitn.ds1.project.message.dss.StartMessage;
import it.unitn.ds1.project.message.dss.Timeout;
import it.unitn.ds1.project.message.dss.decision.Decision;
import it.unitn.ds1.project.message.dss.decision.DecisionRequest;
import it.unitn.ds1.project.message.dss.decision.DecisionResponse;
import it.unitn.ds1.project.message.dss.vote.Vote;
import it.unitn.ds1.project.message.dss.vote.VoteResponse;
import it.unitn.ds1.project.message.txn.begin.TxnBeginMsg;
import it.unitn.ds1.project.message.txn.read.TxnReadRequestMsg;
import it.unitn.ds1.project.message.txn.write.TxnWriteRequestMsg;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class TxnCoordinator extends TxnAbstractNode {

    // here all the nodes that sent YES are collected

    private final Map<Integer, String> transactionMapping = new HashMap<>();

    private final Set<ActorRef> yesVoters = new HashSet<>();

    boolean allVotedYes() { // returns true if all voted YES
        return yesVoters.size() >= N_PARTICIPANTS;
    }

    public TxnCoordinator(int id) {
        super(id);
    }

    static public Props props(int id) {
        return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(id));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Recovery.class, this::onRecovery)
                .match(Timeout.class, this::onTimeout)
                .match(StartMessage.class, this::onStartMessage)
                // CLIENT --> COORDINATOR
                .match(TxnBeginMsg.class, this::onTxnBegin)
                .match(TxnReadRequestMsg.class, this::onTxnReadRequest)
                .match(TxnWriteRequestMsg.class, this::onTxnWriteRequest)

                .match(VoteResponse.class, this::onVoteResponse)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .build();
    }

    private void onTxnReadRequest(TxnReadRequestMsg msg) {
        // TODO
    }
    private void onTxnWriteRequest(TxnWriteRequestMsg msg) {
        //TODO
    }

    public void onStartMessage(StartMessage msg) {                   /* Start */
        setGroup(msg);
        /*print("Sending vote request");
        multicast(new VoteRequest());
        //multicastAndCrash(new VoteRequest(), 3000);
        setTimeout(VOTE_TIMEOUT);
        //crash(5000);*/
    }

    /* On transaction begin message */
    public void onTxnBegin(TxnBeginMsg msg) {
        String transactionID = UUID.randomUUID().toString();
        this.transactionMapping.putIfAbsent(msg.clientId, transactionID);
        // TODO
    }

    public void onVoteResponse(VoteResponse msg) {                    /* Vote */
        if (hasDecided(msg.transactionID)) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }
        Vote v = msg.vote;
        if (v == Vote.YES) {
            yesVoters.add(getSender());
            if (allVotedYes()) {
                fixDecision(msg.transactionID, Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
                //multicast(new DecisionResponse(decision));
                multicastAndCrash(new DecisionResponse(msg.transactionID, decision.get(msg.transactionID)), 3000);
            }
        } else { // a NO vote
            // on a single NO we decide ABORT
            fixDecision(msg.transactionID, Decision.ABORT);
            multicast(new DecisionResponse(msg.transactionID, decision.get(msg.transactionID)));
        }
    }

    public void onTimeout(Timeout msg) {
        if (!hasDecided(msg.transactionID)) {
            print("Timeout. Decision not taken, I'll just abort.");
            fixDecision(msg.transactionID, Decision.ABORT);
            multicast(new DecisionResponse(msg.transactionID, decision.get(msg.transactionID)));
        }
    }

    @Override
    public void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        for (String transactionID : transactionMapping.values()) {
            print("---------" + hasDecided(transactionID) + " - " + decision);
            if (!hasDecided(transactionID)) {
                print("Recovery. Haven't decide, I'll just abort.");
                fixDecision(transactionID, Decision.ABORT);
            }

            multicast(new DecisionResponse(transactionID, decision.get(transactionID)));
        }
        // TODO inform client of decision too
    }

    @Override
    public void crash(int recoverIn) {
            getContext().become(crashed());
            print("CRASH!!!");

            // setting a timer to "recover"
            getContext().system().scheduler().scheduleOnce(
                    Duration.create(recoverIn, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new Recovery(), // message sent to myself
                    getContext().system().dispatcher(), getSelf()
            );
    }
}