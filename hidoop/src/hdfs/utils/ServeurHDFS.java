package hdfs.utils;

import formats.Format;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class ServeurHDFS {
    //args[0] = dossier ou le service enregistre les fragments
    //args[1] = adresse pour acceder au serveur HDFS
    //args[2] = port
    //args[3] nom de la machine
    //args[4] fragment type
    public static void main(String args[]) {
        try {
            int port = Integer.parseInt(args[2]);
            LocateRegistry.createRegistry(port);
            // Create an instance of the server object
            GestionnaireFragments frag;
            switch (args[4])
            {
                case "HD":
                    frag = new GestionnaireFragmentsHardDisk(args[0]);
                    break;
                case "Line":
                    frag = new GestionnaireFragmentsFormat(args[0], Format.Type.LINE);
                    break;
                case "Kv":
                    frag = new GestionnaireFragmentsFormat(args[0], Format.Type.KV);
                    break;
                default :
                    frag = null;
                    break;
            }

            // Register the object with the naming service
            Naming.rebind(args[1]+":"+port+"/serviceHDFS"+args[3], frag);
            System.out.println(" bound in registry");
        } catch (Exception exc) {System.out.println(exc.getMessage()); }
    }
}
