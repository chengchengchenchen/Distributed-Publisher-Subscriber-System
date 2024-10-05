package Broker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import Utils.Broker;
import Utils.Topic;


// Broker Interface
public interface BrokerInterface extends Remote {
    void newBrokerRegistered(Broker broker) throws RemoteException;
}

