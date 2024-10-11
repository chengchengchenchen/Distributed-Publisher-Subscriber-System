/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Publisher;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PublisherInterface extends Remote {
    boolean isAlive() throws RemoteException;
}
