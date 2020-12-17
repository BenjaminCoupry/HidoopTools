package ordo;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

public class ServeurHidoop {

  public static void main(String[] args) {
    //args[0] = adresse pour acceder au serveur Hidoop
    //args[1] = port
    //args[2] = nom de la machine

    try {
      //récupérer le port sur laquelle on lance le registre RMI
      int port = Integer.parseInt(args[1]);
      //créer le registre RMI 
      LocateRegistry.createRegistry(port);
      //créer l'objet serveur
      WorkerInterface worker = new Worker();
      //Enregistrer l'objet dans le registre "//" + args[0] + ":" + port + "/serviceHidoop" + args[2]
      Naming.rebind("//" + args[0] + ":" + port + "/serviceHidoop" + args[2], worker);
      System.out.println(" bound in registry");
    } catch (Exception e) {
      e.printStackTrace();
    }

  } 

}