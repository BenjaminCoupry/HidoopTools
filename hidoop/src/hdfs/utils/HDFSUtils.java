package hdfs.utils;

import java.io.*;
import java.rmi.*;
import java.util.*;
import formats.*;

public class HDFSUtils {

    Nommage noms;
    HashMap<String,GestionnaireFragments> repertoire;
    HashMap<String,String> formats;
    String configuration;

    public HDFSUtils(String config)
    {
        GetterNommage GN = new GetterNommage(config);
        GetterGestionnairesFragments GF = new GetterGestionnairesFragments(config);
        noms = GN.getNommage();
        formats = GF.getFormats();
        configuration = config;
        repertoire = GF.getGestionnaires();
    }

    //Utile pour le groupe HDFS
    //Supprime le fichier nomHDFS du repertoire et des machines
    public void Delete(String nomHDFS)
    {
        try{
            //Recuperer les adresses des blocs
            List<InfoAdresse> blocs = noms.getAdressesFragments(nomHDFS);
            //Supprimer les blocs de la convention de nommage
            noms.supprimerAdressesFragment(nomHDFS);
            for(InfoAdresse i : blocs)
            {
                System.out.println("Suppression de  : "+ i.afficher());
                if(i.getNomFichierComplet().equals(nomHDFS)) {
                    //Supprimer le fragment
                    repertoire.get(i.getNomMachine()).supprimerFragment(i.getNomLocal());
                }
            }
        }catch (Exception e){e.printStackTrace();}
    }

    //Utile pour le groupe HDFS
    //Ajoute la liste de fragments frags au systeme (fichier et nommage) sous le nom nomHDFS
    public void Write(String nomHDFS, List<List<KV>> frags, String ft)
    {
        System.out.println("ecriture d'objet dans HDFS");
        List<String> machines = getNomsMachines(ft);
        int n = 0;
        for(List<KV> o : frags)
        {
            System.out.println("ecriture du fragment "+o);
            try {
                String machine = machines.get(n % machines.size());
                ajouterFragmentSysteme(nomHDFS,machine, (Serializable) o);
                n++;
            }catch(Exception e){e.printStackTrace();}
        }
    }

    //Utile pour le groupe HDFS
    //Renvoie la liste des fragments composant le fichier nomHDFS
    public List<Object> Read(String nomHDFS)
    {
        List<Object> retour = new ArrayList<>();
        try {
            List<InfoAdresse> infos = noms.getAdressesFragments(nomHDFS);
            System.out.println("Les fragments voulus se situent aux lieux suivants : "+infos.toString());
            for(InfoAdresse i : infos)
            {
                System.out.println("Recuperation du fragment "+i+" ...");
                retour.add(recupererFragment(i));
                System.out.println("Recuperation du fragment "+i+" OK !");
            }
        }catch(Exception e){e.printStackTrace();}

        return retour;
    }

    //Renvoie la liste des machines de HDFS
    public List<String> getNomsMachines(String ft)
    {
        System.out.println("Recherche....");
        System.out.println(ft);
        List<Object> total = Arrays.asList(repertoire.keySet().toArray());
        System.out.println("Machines  HDFS totales :");
        System.out.println(total.toString());
        System.out.println("types formats machines :");
        System.out.println(formats.toString());
        List<String> retour = new ArrayList<>();
        for(Object nom_ : total)
        {
            String nom = (String)nom_;
            if(formats.get(nom).equals(ft))
            {
                retour.add(nom);
            }
        }
        return retour;
    }


    //Recupere le fragment associé à l'InfoAdresse donnée
    public Object recupererFragment(InfoAdresse info) throws RemoteException
    {
        System.out.println("Lecture de : "+ info.afficher());
        //SRecuperer le fragment
        Object ob = repertoire.get(info.getNomMachine()).lireFragment(info.getNomLocal());
        return ob;
    }

    //Retourne l'IP de la machine ou est stockee l'info
    public String getIPMachineHDFS(InfoAdresse info) throws FileNotFoundException
    {
        String machine = info.getNomMachine();
        FileInputStream fis=new FileInputStream(configuration);
        Scanner sc =new Scanner(fis);    //file to be scanned
        //returns true if there is another line to read
        while(sc.hasNextLine())
        {
            String ligne = sc.nextLine();   //returns the line that was skipped
            String[] spt = ligne.split(" ");
            if(spt[0].equals("HDFS_Server")) {
                String nom = spt[1];
                String adresse = spt[2];
                String port = spt[3];
                if(nom.equals(machine))
                {
                    return adresse;
                }
            }
        }
        sc.close();     //closes the scanner
        return "";
    }


    //Utile pour le groupe HIDOOP
    //Ajoute le fragment frag au systeme HDFS (et au rep de nommage) pour le fichier nomHDFS, et ce sur la machine précisée
    public void ajouterFragmentSysteme(String nomHDFS,String machine, Serializable frag) throws RemoteException
    {
        System.out.println("ajout d'un fragment de "+nomHDFS +" au systeme sur la machine "+machine);

        GestionnaireFragments gest = repertoire.get(machine);
        System.out.println("gestionnaire de fragments obtenu");
        String nom = gest.ecrireFragment(frag);
        System.out.println("Generation des infos adresse du fragment");
        System.out.println("--------------------------");
        InfoAdresse info = new AdresseFrag(machine,nom, nomHDFS);
        System.out.println("Ecriture Info Adresse : "+ info.afficher());
        noms.enregistrerAdresseFragment(nomHDFS,info);
    }

    //Utile pour le groupe HIDOOP
    //Retourne la liste des  infos concernant les fragments du fichier : infos, ip de la machine de stockage, repertoire de stockage
    public List<InfoEtendue> getAdressesFragments(String nomFichierHDFS) throws RemoteException, FileNotFoundException
    {
        List<InfoEtendue> retour = new ArrayList<>();
        for(InfoAdresse inf : noms.getAdressesFragments(nomFichierHDFS))
        {
            String ip = getIPMachineHDFS(inf);
            GestionnaireFragments gf = repertoire.get(inf.getNomMachine());
            String dir = gf.getDirectory();
            retour.add(new InfoEtendue(inf,ip,dir));
        }
        return retour;
    }
    
}
