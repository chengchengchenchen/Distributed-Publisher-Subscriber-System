package Subscriber;

import Utils.Topic;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberInterface extends Remote {
    void heartbeat() throws RemoteException;
    void noticeTopicDeleted(Topic topic);
}
