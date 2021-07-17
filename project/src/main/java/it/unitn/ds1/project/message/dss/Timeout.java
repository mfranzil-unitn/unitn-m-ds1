package it.unitn.ds1.project.message.dss;

public class Timeout extends DSSMessage {

    public Timeout(String transactionID) {
        super(transactionID);
    }
}
