package Utils;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.Naming;
import java.util.HashMap;
import java.util.Scanner;

public class getterGestionnairesFragments {
    public HashMap<String, GestionnaireFragments> getGestionnaires() {
        return gestionnaires;
    }

    private HashMap<String,GestionnaireFragments> gestionnaires;
    public getterGestionnairesFragments(String configuration)
    {
        gestionnaires = new HashMap<>();
        File config = new File(configuration);
        try {
            FileInputStream fis=new FileInputStream(config);
            Scanner sc=new Scanner(fis);    //file to be scanned
            //returns true if there is another line to read
            while(sc.hasNextLine())
            {
                String ligne = sc.nextLine();   //returns the line that was skipped
                String[] spt = ligne.split(" ");
                if(spt[0].equals("HDFS_Server")) {
                    String nom = spt[1];
                    String adresse = spt[2];
                    String port = spt[3];
                    try {
                        // get the stub of the server object from the rmiregistry
                        GestionnaireFragments gf = (GestionnaireFragments) Naming.lookup(adresse + ":" + port + "/serviceHDFS");
                        gestionnaires.put(nom, gf);
                    } catch (Exception exc) {
                        System.out.println(exc.getMessage());
                    }
                }

            }
            sc.close();     //closes the scanner

        } catch (Exception exc) { System.out.println(exc.getMessage());}
    }

}
