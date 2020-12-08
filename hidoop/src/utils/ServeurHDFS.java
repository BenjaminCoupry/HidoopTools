package Utils;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ServeurHDFS {
    //arg[0] = dossier ou le service enregistre les fragments
    //arg[1] = adresse pour acceder au serveur de Nommage
    //arg[2] = port
    //arg[3] nom de la machine
    public static void main(String args[]) {
        try {
            int port = Integer.parseInt(args[2]);
            Registry registery= LocateRegistry.createRegistry(port);
            // Create an instance of the server object
            GestionnaireFragments frag = new GestionnaireFragmentsHardDisk(args[0]);
            // Register the object with the naming service
            Naming.rebind(args[1]+":"+port+"/serviceHDFS"+args[3], frag);
            System.out.println(" bound in registry");
        } catch (Exception exc) {System.out.println(exc.getMessage()); }
    }
}
