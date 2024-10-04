package Test;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class HelloImpl extends UnicastRemoteObject implements HelloInterface {

    // 必须有一个构造器来声明 RemoteException
    protected HelloImpl() throws RemoteException {
        super();
    }

    // 实现远程接口的方法
    @Override
    public String sayHello(String name) throws RemoteException {
        System.out.println(name);
        return "Hello, " + name + "!";
    }

    // 实现心跳方法
    @Override
    public void heartbeat() throws RemoteException {
        System.out.println("Heartbeat received from publisher.");
    }
}
