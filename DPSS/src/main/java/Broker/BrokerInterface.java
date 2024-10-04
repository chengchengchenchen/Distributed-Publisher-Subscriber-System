package Broker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

import Directory.DirectoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Broker Interface
public interface BrokerInterface extends Remote {
    void newBrokerRegistered(String brokerInfo) throws RemoteException;
}

