/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Directory;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import Utils.Broker;

// Directory Service Interface
public interface DirectoryService extends Remote {
    void registerBroker(Broker broker) throws RemoteException;
    List<Broker> getRegisteredBrokers() throws RemoteException;
}
