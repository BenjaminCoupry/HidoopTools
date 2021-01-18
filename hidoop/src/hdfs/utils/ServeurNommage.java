package hdfs.utils;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class ServeurNommage {
    //arg[0] = dossier ou le service de Nommage enregistre les fragments
    //arg[2] = port
    public static void main(String args[]) {
        try {
            int port = Integer.parseInt(args[1]);
            LocateRegistry.createRegistry(port);
            // Create an instance of the server object
            Nommage nommage = new NommageHardDisk(args[0]);
            System.out.println(InetAddress.getLocalHost().getHostName());
            // Register the object with the naming service
            Naming.rebind("//"+ InetAddress.getLocalHost().getHostName()+":"+port+"/serviceNommageFragments", nommage);
            System.out.println(" bound in registry");
        } catch (Exception exc) {System.out.println(exc.getMessage()); }
    }
}
