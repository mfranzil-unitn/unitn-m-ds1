package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.project.message.TxnBeginMsg;

import java.util.*;

public class TxnCoordinator extends TxnAbstractNode {

    // here all the nodes that sent YES are collected

    private final Map<Integer, String> transactionMapping  = new HashMap<>();

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
                .match(StartMessage.class, this::onStartMessage)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(Timeout.class, this::onTimeout)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(TxnBeginMsg.class, this::onTxnBegin)
                .build();
    }

    public void onStartMessage(StartMessage msg) {                   /* Start */
        setGroup(msg);
        print("Sending vote request");
        multicast(new VoteRequest());
        //multicastAndCrash(new VoteRequest(), 3000);
        setTimeout(VOTE_TIMEOUT);
        //crash(5000);
    }

    /* On transaction begin message */
    public void onTxnBegin(TxnBeginMsg msg){

        String transactionID = UUID.randomUUID().toString();
        this.transactionMapping.putIfAbsent(msg.clientId, transactionID);

    }

    public void onVoteResponse(VoteResponse msg) {                    /* Vote */
        if (hasDecided()) {
            // we have already decided and sent the decision to the group,
            // so do not care about other votes
            return;
        }
        Vote v = msg.vote;
        if (v == Vote.YES) {
            yesVoters.add(getSender());
            if (allVotedYes()) {
                fixDecision(Decision.COMMIT);
                //if (id==-1) {crash(3000); return;}
                //multicast(new DecisionResponse(decision));
                multicastAndCrash(new DecisionResponse(decision), 3000);
            }
        } else { // a NO vote

            // on a single NO we decide ABORT
            fixDecision(Decision.ABORT);
            multicast(new DecisionResponse(decision));
        }
    }

    public void onTimeout(Timeout msg) {
        if (!hasDecided()) {
            print("Timeout. Decision not taken, I'll just abort.");
            fixDecision(Decision.ABORT);
            multicast(new DecisionResponse(decision));
        }
        // print("Timeout");
        // TODONE 1: coordinator timeout action

    }

    @Override
    public void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        print("---------" + hasDecided() + " - " + decision);
        if (!hasDecided()) {
            print("Recovery. Haven't decide, I'll just abort.");
            fixDecision(Decision.ABORT);
        }

        multicast(new DecisionResponse(decision));

        // TODONE 2: coordinator recovery action
    }
}