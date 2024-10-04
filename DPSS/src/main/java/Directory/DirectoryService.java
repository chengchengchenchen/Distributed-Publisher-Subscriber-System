package Directory;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

// Directory Service Interface
public interface DirectoryService extends Remote {
    void registerBroker(String brokerId, String brokerAddress) throws RemoteException;
    List<String> getRegisteredBrokers() throws RemoteException;
}
