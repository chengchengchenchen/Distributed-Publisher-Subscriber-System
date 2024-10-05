package Publisher;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PublisherInterface extends Remote {
    void heartbeat() throws RemoteException;
}
