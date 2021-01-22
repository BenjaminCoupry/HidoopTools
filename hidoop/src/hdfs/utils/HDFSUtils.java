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
        System.out.println("Initialisation d'un utilitaire HDFS");
        //Creer le recuperateur de nommage
        GetterNommage GN = new GetterNommage(config);
        //Creer le recuperateur de gestionnaire de fragments
        GetterGestionnairesFragments GF = new GetterGestionnairesFragments(config);
        //Recuperer le nommage
        noms = GN.getNommage();
        //Recuperer les formats des serveurs
        formats = GF.getFormats();
        configuration = config;
        //Recuperer les gestionnaires de fragments
        repertoire = GF.getGestionnaires();
    }

    //Utile pour le groupe HDFS
    //Supprime le fichier nomHDFS du repertoire et des machines
    public void Delete(String nomHDFS)
    {
        System.out.println("Tentative de suppression HDFS de "+nomHDFS);
        try{
            //Recuperer les adresses des blocs correspondant au nomHDFS
            List<InfoAdresse> blocs = noms.getAdressesFragments(nomHDFS);
            //Supprimer les blocs de la convention de nommage
            System.out.println("Suppression des informations de nommage de  : "+ nomHDFS);
            noms.supprimerAdressesFragment(nomHDFS);
            for(InfoAdresse i : blocs)
            {
                System.out.println("Suppression des fragments HDFS  : "+ i.afficher());
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
        System.out.println("Tentative d'ecriture HDFS de "+nomHDFS);
        //Recuperer les noms des machines capables de gerer le format fourni
        List<String> machines = getNomsMachines(ft);
        Random r = new Random();
        //Initialiser sur une des machines
        int n = r.nextInt(machines.size());;
        //Parcourir les fragments
        for(List<KV> o : frags)
        {
            System.out.println("ecriture du fragment");
            try {
                //Recuperer le nom de la machine n
                String machine = machines.get(n % machines.size());
                //Ajouter le fragment o a la machine n
                ajouterFragmentSysteme(nomHDFS,machine, (Serializable) o);
                n++;
            }catch(Exception e){e.printStackTrace();}
        }
    }

    //Utile pour le groupe HDFS
    //Renvoie la liste des fragments composant le fichier nomHDFS
    public List<Object> Read(String nomHDFS)
    {
        System.out.println("Tentative de lecture HDFS de "+nomHDFS);
        //Initialiser la liste des fragments de retour
        List<Object> retour = new ArrayList<>();
        try {
            //Recuperer la liste des infos pour trouver les fragments
            List<InfoAdresse> infos = noms.getAdressesFragments(nomHDFS);
            System.out.println("Les fragments voulus se situent aux lieux suivants : "+infos.toString());
            for(InfoAdresse i : infos)
            {
                System.out.println("Recuperation du fragment "+i+" ...");
                //Recuperer le fragment correspondant a l'info adresse i
                retour.add(recupererFragment(i));
                System.out.println("Recuperation du fragment OK !");
            }
        }catch(Exception e){e.printStackTrace();}

        return retour;
    }

    //Renvoie la liste des machines de HDFS gerant le format specifie
    public List<String> getNomsMachines(String ft)
    {
        System.out.println("Recherche des machines HDFS au format "+ft);
        //Recuperer le nom des machines dans leur entierete
        List<Object> total = Arrays.asList(repertoire.keySet().toArray());
        List<String> retour = new ArrayList<>();
        for(Object nom_ : total)
        {
            String nom = (String)nom_;
            //Recuperer le format que gere la machine
            String formatmachine = formats.get(nom);
            //Comparer avec le format souhaite
            if(formatmachine.equals(ft))
            {
                //Ajouter le nom de la machine au resultat
                retour.add(nom);
            }
        }
        return retour;
    }


    //Recupere le fragment associé à l'InfoAdresse donnée
    public Object recupererFragment(InfoAdresse info) throws RemoteException
    {
        System.out.println("Recuperation du fragment associe a l'information  : "+ info.afficher());
        //Recuperer le nom de la machine stockant le fragment
        String nomMachine = info.getNomMachine();
        //Recuperer le gestionnaire de fragments de cette machine
        GestionnaireFragments gf = repertoire.get(nomMachine);
        //Recuperer le nom du fichier contenant le fragment sur la machine en question
        String nomLocalFragment = info.getNomLocal();
        //Lire ce fragment distant
        Object ob = gf.lireFragment(nomLocalFragment);
        return ob;
    }

    //Retourne l'IP de la machine ou est stockee l'info
    public String getIPMachineHDFS(InfoAdresse info) throws FileNotFoundException
    {
        //Recuperer le nom de la machine
        String machine = info.getNomMachine();
        //Lire le fichier de configuration
        FileInputStream fis=new FileInputStream(configuration);
        Scanner sc =new Scanner(fis);
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
        //Recuperer le gestionnaire de fragment associé a la machine donnée en argument
        GestionnaireFragments gest = repertoire.get(machine);
        //Le frahment est enregistré par le gestionnaire sous le nom donné
        String nom = gest.ecrireFragment(frag);
        System.out.println("Generation des infos adresse du fragment");
        //On pack les infos
        InfoAdresse info = new AdresseFrag(machine,nom, nomHDFS);
        //Ajout des infos de ce fragment au sserveur de nommage
        noms.enregistrerAdresseFragment(nomHDFS,info);
    }

    //Utile pour le groupe HIDOOP
    //Retourne la liste des  infos concernant les fragments du fichier : infos, ip de la machine de stockage, repertoire de stockage
    public List<InfoEtendue> getAdressesFragments(String nomFichierHDFS) throws RemoteException, FileNotFoundException
    {
        List<InfoEtendue> retour = new ArrayList<>();
        //Recuperer la liste des infos correspondant au fichier HDFS spécifié
        List<InfoAdresse> LI =  noms.getAdressesFragments(nomFichierHDFS);
        //Parcourir les infos
        for(InfoAdresse inf :LI)
        {
            //Recuperer l'IP de la machine ou est stocke le fragment
            String ip = getIPMachineHDFS(inf);
            //Recuperer le gestionnaire de fragment de cette machine
            GestionnaireFragments gf = repertoire.get(inf.getNomMachine());
            //Obtenir le repertoire de travail de ce gestionnaire
            String dir = gf.getDirectory();
            //Pack les infos et les ajouter au resultat
            retour.add(new InfoEtendue(inf,ip,dir));
        }
        return retour;
    }
    
}
