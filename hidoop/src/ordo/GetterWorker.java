package ordo;

import java.io.*;
import java.rmi.Naming;
import java.util.*;

public class GetterWorker {

    private HashMap<String, WorkerInterface> workers;

    public GetterWorker(String configuration) {
        File config = new File(configuration);
        this.workers = new HashMap<String, WorkerInterface>();

        try {
            FileInputStream fis = new FileInputStream(config);
            Scanner sc = new Scanner(fis);    //fichier à lire
            //vrai si il y a une autre ligne à lire
            while(sc.hasNextLine())
            {
                String ligne = sc.nextLine();   //la ligne que l'on vient de lire
                String[] spt = ligne.split(" ");                
                if(spt[0].equals("Hidoop")) {
                    String nom = spt[1];
                    String adresse = spt[2];
                    String port = spt[3];
                    try {
                        //récupérer le stub
                        WorkerInterface w  = (WorkerInterface) Naming.lookup("//" + adresse + ":" + port + "/serviceHidoop" + nom);
                        this.workers.put(nom, w);
                        System.out.println("Serveur Hidoop " + nom + " trouvé !");
                    } catch (Exception exc) {
                        System.out.println(exc.getMessage());
                        exc.printStackTrace();
                    }
                    break;
                }
            }
            sc.close();     //closes the scanner
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public WorkerInterface getWorker(String nomMachine) {
        return this.workers.get(nomMachine);
    }
}