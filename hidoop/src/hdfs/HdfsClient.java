/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.utils.HDFSUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    //Supprimer le fichier de nom hdfs hdfsFname de HDFS
    public static void HdfsDelete(String hdfsFname) {
        //Recuperer l'utilitaire
        HDFSUtils utils = new HDFSUtils(Project.PATH_CONFIG);
        //Supprimer de HDFS le fichier
        utils.Delete(hdfsFname);
    }
	//Copier un fichier local de chemin localFSSourceFname et de format fmt dans HDFS
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) {
        //Initialiser ce qui recevra les KV du fichier local
        List<KV> fichLoc = new ArrayList<>();
        //Initialiser l'utilitaire
        HDFSUtils utils = new HDFSUtils(Project.PATH_CONFIG);
        Format format_reel = null;
        String strFmt = "";
        //Recuperation du format pour lire le fichier
        if (fmt.equals(Format.Type.KV))
        {
            format_reel = new KVFormat(localFSSourceFname);
            strFmt = "Kv";
        }
        else if(fmt.equals(Format.Type.LINE))
        {
            format_reel = new LineFormat(localFSSourceFname);
            strFmt = "Line";
        }
        format_reel.open(Format.OpenMode.R);
        KV lu;
        do {
            lu = format_reel.read();
            if(lu!=null)
            {
                fichLoc.add(new KV(lu.k,lu.v));
            }
        }while(lu!=null);
        //Format pour enregistrer dans HDFS
        String format_HDFS = "Kv";
        //Fermer le format de lecture
        format_reel.close();
        System.out.println("Fichier local lu");
        //Decouper en fragments
        List<List<KV>> frags = Splitter(fichLoc,repFactor,utils,format_HDFS);
        //Generer un nom hdfs
        String nomHDFS = Paths.get(localFSSourceFname).getFileName().toString();
        //enregistrer e fichier HDFS
        System.out.println("Le fichier local "+localFSSourceFname + " va etre enregistré dans HDFS sous le nom "+ nomHDFS);
        System.out.println("Ce fichier est composé de "+frags.size()+"fragments.");
        utils.Write(nomHDFS,frags,format_HDFS);
        System.out.println(""+localFSSourceFname + " ===> "+ nomHDFS+" : OK !");
    }

    //Decoupe une List<KV> en List<List<KV>> , avec une redondance de repet, avec autant de fragments que
    // de serveurs correspondant au format specifie
    private static List<List<KV>> Splitter(List<KV> lkv, int repet, HDFSUtils utils, String strfmt)
    {
        //La taille minimale en KV d'un fragment
        int tailleFragMin = 5;
        //Initialiser la liste de fragments qui sera renvoyée
        List<List<KV>> ret = new ArrayList<>();
        //Recevoir le nombre de machines correspondant au format spécifié
        int nbserv = utils.getNomsMachines(strfmt).size();
        //Initialiser les fragments
        for(int i=0;i<nbserv;i++)
        {
            ret.add(new ArrayList<>());
        }
        //Initialisation
        int i= 0;
        for(KV kv : lkv)
        {
            for(int j=0;j<repet;j++)
            {
                //Parcourir toutes les machines succesivement
                ret.get(i%nbserv).add(kv);
                i++;
            }
        }
        //S'assurer d'une taille de fragments minimale
        List<List<KV>> ret2 = new ArrayList<>();
        List<KV> setup = new ArrayList<>();
        for(List<KV> lkv2 : ret)
        {
            //Aggreger les fragments jusqu a obtenir une taille satisfaisante
            setup.addAll(lkv2);
            if(setup.size()>=tailleFragMin)
            {
                ret2.add(setup);
                setup = new ArrayList<>();
            }
        }
        //Ajouter le dernier fragment trop petit
        if(setup.size()<tailleFragMin && setup.size()>0)
        {
            ret2.add(setup);
        }
        return ret2;
    }

    //Lire le fichier HDFS de nom hdfsFname et l'enregistrer sous forme de KV
    // localement dans le repertoire localFSDestFname
    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        //Initialiser les utils
        HDFSUtils utils = new HDFSUtils(Project.PATH_CONFIG);
        //Lire la liste des fragments
        Object lu =  utils.Read(hdfsFname);
        if(lu instanceof List)
        {
            //Lu est une liste de fragments
            List<List<KV>> llkv = (List<List<KV>>) lu;
            //On decide d'ecrire localement en KV
            Format format_reel = new KVFormat(localFSDestFname);
            //Ce sera une ecriture
            format_reel.open(Format.OpenMode.W);
            for(List<KV> lkv : llkv)
            {
                //Ecrire tous les fragments
                for(KV kv : lkv)
                {
                    //Ecrire toutes les KV du fragment
                    format_reel.write(kv);
                }
            }
            //Fermer le format
            format_reel.close();
        }
    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
