package ordo;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class ServeurHidoop {

  public static void main(String[] args) {
    //args[0] = port
    //args[1] = nom de la machine

    try {
      //récupérer le port sur laquelle on lance le registre RMI
      int port = Integer.parseInt(args[0]);
      //créer le registre RMI 
      LocateRegistry.createRegistry(port);
      //créer l'objet serveur
      WorkerInterface worker = new Worker();
      //Enregistrer l'objet dans le registre
      Naming.rebind("//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/serviceHidoop" + args[1], worker);
      System.out.println(" bound in registry");
    } catch (Exception e) {
      e.printStackTrace();
    }

  } 

}