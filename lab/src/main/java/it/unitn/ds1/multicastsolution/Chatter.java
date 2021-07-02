package it.unitn.ds1.multicastsolution;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

class Chatter extends AbstractActor {

    // number of chat messages to send
    final static int N_MESSAGES = 5;
    private final Random rnd = new Random();
    private List<ActorRef> group; // the list of peers (the multicast group)
    private int sendCount = 0;    // number of sent messages
    private final String myTopic;       // The topic I am interested in, null if no topic
    private final int id;         // ID of the current actor
    private int[] vc;             // the local vector clock

    // a buffer storing all received chat messages
    private final StringBuffer chatHistory = new StringBuffer();

    // TODO 2: provide a buffer for out-of-order messages

    // message queue to hold out-of-order messages
    private final List<ChatMsg> mq = new ArrayList<>();

    /* -- Message types ------------------------------------------------------- */

    // Start message that informs every chat participant about its peers
    public static class JoinGroupMsg implements Serializable {
        private final List<ActorRef> group; // list of group members

        public JoinGroupMsg(List<ActorRef> group) {
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }

    // A message requesting the peer to start a discussion on his topic
    public static class StartChatMsg implements Serializable {
    }

    // Chat message
    public static class ChatMsg implements Serializable {
        public final String topic;   // "topic" of the conversation
        public final int n;          // the number of the reply in the current topic
        public final int senderId;   // the ID of the message sender
        public final int[] vc;       // vector clock

        public ChatMsg(String topic, int n, int senderId, int[] vc) {
            this.topic = topic;
            this.n = n;
            this.senderId = senderId;
            this.vc = new int[vc.length];
            for (int i = 0; i < vc.length; i++) this.vc[i] = vc[i];
        }
    }

    // A message requesting to print the chat history
    public static class PrintHistoryMsg implements Serializable {
    }

    /* -- Actor constructor --------------------------------------------------- */

    public Chatter(int id, String topic) {
        this.id = id;
        this.myTopic = topic;
    }

    static public Props props(int id, String topic) {
        return Props.create(Chatter.class, () -> new Chatter(id, topic));
    }

    /* -- Actor behaviour ----------------------------------------------------- */
    private void sendChatMsg(String topic, int n) {
        sendCount++;

        // TODO 3: update vector clock

        // increase vector clock value assigned to this actor
        this.vc[id]++;

        // generate chat message
        ChatMsg m = new ChatMsg(topic, n, this.id, this.vc);
        System.out.printf("%02d: %s%02d\n", this.id, topic, n);

        // send to peers and append to log
        multicast(m);
        appendToHistory(m);
    }

    private void multicast(Serializable m) {

        // randomly arrange peers
        List<ActorRef> shuffledGroup = new ArrayList<>(group);
        Collections.shuffle(shuffledGroup);

        // multicast to all peers in the group (do not send any message to self)
        for (ActorRef p : shuffledGroup) {
            if (!p.equals(getSelf())) {
                p.tell(m, getSelf());

                // simulate network delays using sleep
                try {
                    Thread.sleep(rnd.nextInt(10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

  /*
  private void multicast(Serializable m) {
    for (ActorRef p: group) {
      if (!p.equals(getSelf()))
      p.tell(m, getSelf());
    }
  }
  */

    // here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class, this::onJoinGroupMsg)
                .match(StartChatMsg.class, this::onStartChatMsg)
                .match(ChatMsg.class, this::onChatMsg)
                .match(PrintHistoryMsg.class, this::printHistory)
                .build();
    }

    private void onJoinGroupMsg(JoinGroupMsg msg) {
        this.group = msg.group;

        // create the vector clock
        this.vc = new int[this.group.size()];
        System.out.printf("%s: joining a group of %d peers with ID %02d\n",
                getSelf().path().name(), this.group.size(), this.id);
    }

    private void onStartChatMsg(StartChatMsg msg) {

        // start topic with message 0
        sendChatMsg(myTopic, 0);
    }

    private void onChatMsg(ChatMsg msg) {

        // TODO 4: deliver only if the message is in-order
        // TODO 4 hint: once a message is delivered, update the vector clock...

        // check if message can be delivered (i.e., it is in-order)
        if (canDeliver(msg)) {
            while (msg != null) {

                // update vector clock and deliver message
                updateLocalClock(msg);
                deliver(msg);

                // the vector clock was updated, try to find another in-order message
                msg = findDeliverable();
            }
        } else {

            // cannot deliver the message right now, putting it on hold
            this.mq.add(msg);
            System.out.printf("%02d: enqueue from %02d %s local: %s queue length: %d\n",
                    this.id, msg.senderId,
                    Arrays.toString(msg.vc), Arrays.toString(this.vc),
                    mq.size());
        }
    }

    // find a message in the queue that can be delivered now;
    // if found, remove it from the queue and return it
    private ChatMsg findDeliverable() {
        Iterator<ChatMsg> I = mq.iterator();
        while (I.hasNext()) {
            ChatMsg m = I.next();
            if (canDeliver(m)) {
                I.remove();
                return m;
            }
        }
        return null;        // nothing can be delivered right now
    }

    private boolean canDeliver(ChatMsg incoming) {
        if (incoming.vc[incoming.senderId] != vc[incoming.senderId] + 1)
            return false;

        for (int i = 0; i < vc.length; i++) {
            if (i != incoming.senderId && incoming.vc[i] > vc[i])
                return false;
        }
        return true;
    }

    private void updateLocalClock(ChatMsg m) {
        for (int i = 0; i < vc.length; i++)
            vc[i] = (m.vc[i] > vc[i]) ? m.vc[i] : vc[i];
    }

    private void deliver(ChatMsg m) {

        // our "chat application" appends all the received messages to the
        // chatHistory and replies if the topic of the message is interesting
        appendToHistory(m);

        // if the message is on my topic and I still have something to say...
        if (m.topic.equals(myTopic) && sendCount < N_MESSAGES) {

            // reply to the received message with an incremented value and the same topic
            sendChatMsg(m.topic, m.n + 1);
        }
    }

    private void appendToHistory(ChatMsg m) {
        chatHistory.append(m.topic).append(m.n).append(" ");
    }

    private void printHistory(PrintHistoryMsg msg) {
        System.out.printf("%02d: %s\n", this.id, chatHistory);
    }
}
