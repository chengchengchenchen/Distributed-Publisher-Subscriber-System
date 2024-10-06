package Broker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Utils.Broker;
import Utils.Publisher;
import Utils.Topic;


// Broker Interface
public interface BrokerInterface extends Remote {
    void newBrokerRegistered(Broker broker) throws RemoteException;
    void newPublisherConnected(Publisher publisher) throws RemoteException;

    // Publisher 创建 Topic
    void createTopic(Publisher publisher, Topic topic) throws RemoteException;

    void printTest(String input) throws RemoteException;
}

