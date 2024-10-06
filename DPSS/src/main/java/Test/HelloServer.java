package Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class HelloServer {
    public static void main(String[] args) {
        try {
            // 创建远程对象实例
            HelloImpl hello = new HelloImpl();
            int assignedPort;
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                assignedPort = serverSocket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println(assignedPort);
            Registry registry = LocateRegistry.createRegistry(assignedPort);

            // 将远程对象绑定到注册表中
            registry.rebind("HelloService", hello);

            System.out.println("HelloService 已启动...");
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
