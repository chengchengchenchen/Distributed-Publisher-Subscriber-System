package Subscriber;

import Utils.Topic;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
@Slf4j
public class SubscriberImpl extends UnicastRemoteObject implements SubscriberInterface {
    protected SubscriberImpl() throws RemoteException {
        super();
    }

    // Heartbeat
    @Override
    public void heartbeat() throws RemoteException {
        log.info("Heartbeat sent from subscriber.");
    }

    @Override
    public void noticeTopicDeleted(Topic topic) {
        log.info("Topic deleted: " + topic);
    }
}
