package Test;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface HelloInterface extends Remote {
    String sayHello(String name) throws RemoteException;

    void heartbeat() throws RemoteException;
}
