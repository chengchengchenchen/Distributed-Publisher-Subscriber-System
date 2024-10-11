/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Publisher;

import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
@Slf4j
public class PublisherImpl extends UnicastRemoteObject implements PublisherInterface {
    protected PublisherImpl() throws RemoteException {
        super();
    }

    @Override
    public boolean isAlive() throws RemoteException {
        //log.info("Publisher is alive.");
        return true;
    }
}
