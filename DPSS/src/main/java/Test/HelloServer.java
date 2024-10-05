package Test;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class HelloServer {
    public static void main(String[] args) {
        try {
            // 创建远程对象实例
            HelloImpl hello = new HelloImpl();

            // 创建本地 RMI 注册表，默认端口 1099
            Registry registry = LocateRegistry.createRegistry(1100);

            // 将远程对象绑定到注册表中
            registry.rebind("HelloService", hello);

            System.out.println("HelloService 已启动...");
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
