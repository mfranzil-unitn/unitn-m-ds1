package it.unitn.ds1.project;

import akka.actor.ActorRef;
import akka.actor.Props;

import it.unitn.ds1.project.message.TxnReadMsg;
import it.unitn.ds1.project.message.TxnReadResponseMsg;
import it.unitn.ds1.project.message.TxnWriteMsg;
import it.unitn.ds1.project.model.DataItem;
import it.unitn.ds1.project.model.PrivateWorkspace;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.util.Random;

/*-- The data store -----------------------------------------------------------*/
public class TxnDSS extends TxnAbstractNode {
    private final double COMMIT_PROBABILITY = 0.999;

    private final Map<Integer, DataItem> items = new HashMap<>();
    private final Map<String, PrivateWorkspace> privateWorkspaces = new HashMap<>();
    private final Map<String, List<DataItem>> lockedItems = new HashMap();

    private ActorRef coordinator;
    private Vote vote;
    private final Random r;

    public TxnDSS(int id, int lowerBound) {
        super(id);
        this.vote = null;
        this.r = new Random();

        for( int i = lowerBound; i < lowerBound + 10; i++ ){
            this.items.put(i, new DataItem(r.nextInt(), 1));
        }

    }

    static public Props props(int id, int lowerBound) {
        return Props.create(TxnDSS.class, () -> new TxnDSS(id, lowerBound));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::onStartMessage)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .match(Timeout.class, this::onTimeout)
                .match(Recovery.class, this::onRecovery)
                .match(TxnReadMsg.class, this::onTxnReadMessage)
                .match(TxnWriteMsg.class, this::onTxnWriteMessage)
                .build();
    }

    public void onTxnReadMessage(TxnReadMsg msg){

        ActorRef sender = this.getSender();

        PrivateWorkspace currentPrivateWorkspace = this.privateWorkspaces.get(msg.transactionID);
        if( currentPrivateWorkspace == null ){
            currentPrivateWorkspace = new PrivateWorkspace();
            DataItem copy = new DataItem(this.items.get(msg.key));
            currentPrivateWorkspace.put(msg.key, copy);
        }

        DataItem copiedItem = currentPrivateWorkspace.get(msg.key);

        TxnReadResponseMsg responseMsg = new TxnReadResponseMsg(msg.transactionID, copiedItem.getValue());

        sender.tell(responseMsg, this.getSelf());

    }

    public void onTxnWriteMessage(TxnWriteMsg msg){

        PrivateWorkspace currentPrivateWorkspace = this.privateWorkspaces.get(msg.transactionID);
        if( currentPrivateWorkspace == null ){
            currentPrivateWorkspace = new PrivateWorkspace();
            DataItem copy = new DataItem(this.items.get(msg.key));
            currentPrivateWorkspace.put(msg.key, copy);
        }

        currentPrivateWorkspace.get(msg.key).setValue(msg.value);

    }

    public void onStartMessage(StartMessage msg) {
        setGroup(msg);
    }

    public void onVoteRequest(VoteRequest msg) {

        this.coordinator = getSender();
        Vote v;
        PrivateWorkspace currentPrivateWorkspace =
                this.privateWorkspaces.getOrDefault(msg.transactionID, new PrivateWorkspace());

        boolean commit = true;
        List<DataItem> locked = new ArrayList<>();

        for( Map.Entry<Integer, DataItem> modifiedEntry : currentPrivateWorkspace.entrySet() ){

            DataItem originalDataItem = this.items.get(modifiedEntry.getKey());
            if( originalDataItem.acquireLock() && originalDataItem.getVersion() == modifiedEntry.getValue().getVersion() - 1 ){
                locked.add(originalDataItem);
            } else {
                commit = false;
            }

        }

        if( !commit ){
            this.lockedItems.put(msg.transactionID,locked);

            // TODO: Add self-sending an abort message
            //this.getSelf().tell(new Abort)

            v = Vote.NO;
        } else {

            v = Vote.YES;

        }

        // TODO: Forward decision to coordinator and remember



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
        vote = r.nextDouble() < COMMIT_PROBABILITY ? Vote.YES : Vote.NO;

        if (vote == Vote.NO) {
            fixDecision(Decision.ABORT);
        }
        print("sending vote " + vote);
        this.coordinator.tell(new VoteResponse(vote), getSelf());
        setTimeout(DECISION_TIMEOUT);
    }

    public void onTimeout(Timeout msg) {
        // TODONE 3: participant termination protocol

        // we assume that vote request arrives sooner or later so no forced abort

        print("---------" + hasDecided() + " - " + vote);
        if (!hasDecided()) {
            if (vote == Vote.YES) {
                print("Timeout. Asking around.");
                multicast(new DecisionRequest());
            }
        }
    }

    @Override
    public void onRecovery(Recovery msg) {
        getContext().become(createReceive());

        // We don't handle explicitly the "not voted" case here
        // (in any case, it does not break the protocol)
        if (!hasDecided()) {
            print("Recovery. Asking the coordinator.");
            coordinator.tell(new DecisionRequest(), getSelf());
            setTimeout(DECISION_TIMEOUT);
        }
    }

    public void onDecisionResponse(DecisionResponse msg) { /* Decision Response */

        if( msg.decision != null ){

            switch(msg.decision){

                case COMMIT:

                    this.privateWorkspaces.get(msg.transactionID).entrySet().forEach( entry -> {

                        this.items.get(entry.getKey()).setValue(entry.getValue().getValue());
                        this.items.get(entry.getKey()).setVersion(entry.getValue().getVersion());

                    });

                case ABORT:
                    this.lockedItems.getOrDefault(msg.transactionID, new ArrayList<>()).forEach(DataItem::releaseLock);
                    this.lockedItems.remove(msg.transactionID);


            }

        }

        // TODO: store the decision for every transaction
        // fixDecision(msg.decision);
    }
}