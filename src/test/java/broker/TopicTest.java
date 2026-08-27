package broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TopicTest {

    @Test
    void retainedMessageIsStoredAndReturnedAsDefensiveCopies() {
        Topic topic = new Topic("weather");
        byte[] retained = new byte[]{1, 2, 3};

        topic.setRetainedMessage(retained);
        retained[0] = 9;

        byte[] firstRead = topic.getRetainedMessage();
        assertArrayEquals(new byte[]{1, 2, 3}, firstRead);

        firstRead[1] = 8;
        byte[] secondRead = topic.getRetainedMessage();
        assertArrayEquals(new byte[]{1, 2, 3}, secondRead);
    }

    @Test
    void addAndRemoveSubscriberUpdatesMembership() {
        Topic topic = new Topic("weather");

        topic.addSubscriber("client-1");
        assertTrue(topic.getSubscribers().contains("client-1"));

        topic.removeSubscriber("client-1");
        assertFalse(topic.getSubscribers().contains("client-1"));
    }

    @Test
    void canBeRemovedOnlyWhenNoSubscribersAndNoRetainedMessage() {
        Topic topic = new Topic("weather");

        assertTrue(topic.canBeRemoved());

        topic.addSubscriber("client-1");
        assertFalse(topic.canBeRemoved());

        topic.removeSubscriber("client-1");
        topic.setRetainedMessage(new byte[]{7});
        assertFalse(topic.canBeRemoved());

        topic.clearRetainedMessage();
        assertTrue(topic.canBeRemoved());
    }
}

