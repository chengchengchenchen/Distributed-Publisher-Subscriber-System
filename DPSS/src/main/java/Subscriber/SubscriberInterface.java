package Subscriber;

import Utils.Message;
import Utils.Topic;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberInterface extends Remote {
    boolean isAlive() throws RemoteException;
    void notifyTopicDeleted(Topic topic) throws RemoteException;
    void receiveMessage(Message message) throws RemoteException;
}
