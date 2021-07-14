package it.unitn.ds1.project.message;


import java.io.Serializable;

// message from the coordinator to the client with the outcome of the TXN
public class TxnResultMsg implements Serializable {
    public final Boolean commit; // if false, the transaction was aborted

    public TxnResultMsg(boolean commit) {
        this.commit = commit;
    }
}

