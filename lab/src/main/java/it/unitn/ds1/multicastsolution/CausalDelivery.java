package it.unitn.ds1.multicastsolution;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CausalDelivery {
    final private static int N_LISTENERS = 10; // number of listening actors

    public static void main(String[] args) {
        // Create the 'helloakka' actor system
        final ActorSystem system = ActorSystem.create("helloakka");

        List<ActorRef> group = new ArrayList<>();
        int id = 0;

        // the first four peers will be participating in conversations
        group.add(system.actorOf(
                Chatter.props(id++, "a"),  // this one will start the topic "a"
                "chatter0"));

        group.add(system.actorOf(
                Chatter.props(id++, "a"), // this one will catch up the topic "a"
                "chatter1"));

        group.add(system.actorOf(
                Chatter.props(id++, "."),  // this one will start the topic "a"
                "chatter2"));

        group.add(system.actorOf(
                Chatter.props(id++, "."), // this one will catch up the topic "a"
                "chatter3"));

        // the rest are silent listeners: they don't have topics to discuss
        for (int i = 0; i < N_LISTENERS; i++) {
            group.add(system.actorOf(Chatter.props(id++, null), "listener" + i));
        }

        // ensure that no one can modify the group
        group = Collections.unmodifiableList(group);

        // send the group member list to everyone in the group
        Chatter.JoinGroupMsg join = new Chatter.JoinGroupMsg(group);
        for (ActorRef peer : group) {
            peer.tell(join, ActorRef.noSender());
        }

        // tell the first chatter to start conversation
        group.get(0).tell(new Chatter.StartChatMsg(), ActorRef.noSender());
        group.get(2).tell(new Chatter.StartChatMsg(), ActorRef.noSender());
        try {
            System.out.println(">>> Wait for the chats to stop and press ENTER <<<");
            System.in.read();

            Chatter.PrintHistoryMsg msg = new Chatter.PrintHistoryMsg();
            for (ActorRef peer : group) {
                peer.tell(msg, ActorRef.noSender());
            }
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        system.terminate();
    }
}
