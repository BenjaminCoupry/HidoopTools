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

public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
        HDFSUtils utils = new HDFSUtils(Project.PATH_CONFIG);
        utils.Delete(hdfsFname);
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) {
        List<KV> fichLoc = new ArrayList<>();

        HDFSUtils utils = new HDFSUtils(Project.PATH_CONFIG);
        Format format_reel = null;
        String strFmt = "";
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
        format_reel.close();
        System.out.println("Fichier local lu");
        System.out.println(fichLoc.toString());
        List<List<KV>> frags = Splitter(fichLoc,repFactor,utils,strFmt);
        System.out.println("Decoupage en "+frags.size()+" fragments");
        System.out.println(frags.toString());

        utils.Write(Paths.get(localFSSourceFname).getFileName().toString(),frags,strFmt);
    }

    private static List<List<KV>> Splitter(List<KV> lkv, int repet, HDFSUtils utils, String strfmt)
    {
        int tailleFragMin = 5;
        List<List<KV>> ret = new ArrayList<>();
        int nbserv = utils.getNomsMachines(strfmt).size();
        System.out.println("Répartition du fichier entre les machines de format "+strfmt);
        System.out.println(nbserv + " machines trouvées");
        for(int i=0;i<nbserv;i++)
        {
            ret.add(new ArrayList<>());
        }
        int i=0;
        for(KV kv : lkv)
        {
            for(int j=0;j<repet;j++)
            {
                ret.get(i%nbserv).add(kv);
                i++;
            }
        }
        //S'assurer d'une taille de fragments minimale
        List<List<KV>> ret2 = new ArrayList<>();
        List<KV> setup = new ArrayList<>();
        for(List<KV> lkv2 : ret)
        {
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

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        HDFSUtils utils = new HDFSUtils(Project.PATH_CONFIG);
        Object lu =  utils.Read(hdfsFname);
        if(lu instanceof List)
        {
            List<List<KV>> llkv = (List<List<KV>>) lu;
            Format format_reel = new KVFormat(localFSDestFname);
            format_reel.open(Format.OpenMode.W);
            for(List<KV> lkv : llkv)
            {
                for(KV kv : lkv)
                {
                    format_reel.write(kv);
                }
            }
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
