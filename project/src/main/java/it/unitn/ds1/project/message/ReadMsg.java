package it.unitn.ds1.project.message;


import java.io.Serializable;

// READ request from the client to the coordinator
public class ReadMsg implements Serializable {
    public final Integer clientId;
    public final Integer key; // the key of the value to read

    public ReadMsg(int clientId, int key) {
        this.clientId = clientId;
        this.key = key;
    }
}
