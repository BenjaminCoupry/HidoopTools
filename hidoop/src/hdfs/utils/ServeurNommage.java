package hdfs.utils;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class ServeurNommage {
    //arsg[0] = dossier ou le service de Nommage enregistre les fragments
    //args[1] = port
    //args[2] = adresse
    public static void main(String args[]) {
        try {
            int port = Integer.parseInt(args[1]);
            LocateRegistry.createRegistry(port);
            // Create an instance of the server object
            Nommage nommage = new NommageHardDisk(args[0]);
            // Register the object with the naming service
            Naming.rebind("//"+args[2]+":"+port+"/serviceNommageFragments", nommage);
            System.out.println(" bound in registry");
        } catch (Exception exc) {System.out.println(exc.getMessage()); }
    }
}
