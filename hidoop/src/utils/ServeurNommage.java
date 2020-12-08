package Utils;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ServeurNommage {
    //arg[0] = dossier ou le service de Nommage enregistre les fragments
    //arg[1] = adresse pour acceder au serveur de Nommage
    //arg[2] = port
    public static void main(String args[]) {
        try {
            int port = Integer.parseInt(args[2]);
            Registry registery= LocateRegistry.createRegistry(port);
            // Create an instance of the server object
            Nommage nommage = new NommageHardDisk(args[0]);
            // Register the object with the naming service
            Naming.rebind(args[1]+":"+port+"/serviceNommageFragments", nommage);
            System.out.println(" bound in registry");
        } catch (Exception exc) {System.out.println(exc.getMessage()); }
    }
}
