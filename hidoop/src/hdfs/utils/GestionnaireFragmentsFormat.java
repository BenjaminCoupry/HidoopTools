package hdfs.utils;

//import com.sun.istack.internal.Nullable;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

import java.io.*;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class GestionnaireFragmentsFormat extends UnicastRemoteObject implements GestionnaireFragments{

    private static final long serialVersionUID = 1L;
    String directory;
    Format.Type ft;
    private static final Pattern ext = Pattern.compile("(?<=.)\\.[^.]+$");


    public GestionnaireFragmentsFormat(String directory, Format.Type f)throws RemoteException {
        this.directory = directory;
        ft = f;
    }

    public static String getFileNameWithoutExtension(File file) {
        return ext.matcher(file.getName()).replaceAll("");
    }

    //Recupere tous les fichiers de type .fragment du repertoire de travail
    private List<File> getFichInDir()
    {
        File[] folder = new File(directory).listFiles();
        List<File> results = new ArrayList<File>();
        if(folder == null)
        {
            return new ArrayList<>();
        }
        else
        {
            for (File file : folder) {
                if (file.isFile()) {
                    if(file.getName().contains(".fragment")) {
                        results.add(file);
                    }
                }
            }
            return results;
        }
    }

    //@Nullable
    //Recupere le fichier du fragment a partir du nom du fragment (sans extension)
    private File getFichNom(String nomFichier)
    {
        List<File> inDir = getFichInDir();
        for(File nf : inDir)
        {
            String pathSolo = Paths.get(nf.getName()).getFileName().toString();
            if(pathSolo.equals(nomFichier))
            {
                return nf;
            }
        }
        return null;
    }

    //contenu doit etre une liste de KV
    //Ecrit dans un nouveau fichier le fragment fourni
    @Override
    public String ecrireFragment(Serializable contenu) {
        String nom = getNomNouveauFichier()+".fragment";
        enregistrerFragment(contenu,new File(directory+"/"+ nom));
        return nom;
    }

    @Override
    //Recupere le fragment a partir du nom du fragment (sans extension)
    public Object lireFragment(String nom) {
        return lireFragment(getFichNom(nom));
    }

    //Enregistre un fragment dans un fichier fourni
    private void enregistrerFragment(Serializable frag , File f)
    {
        try {
            List<KV> kvs = (List<KV>)frag;
            Format format_reel;
            if (ft.equals(Format.Type.KV))
            {
                format_reel = new KVFormat(f.getPath());

            }
            else if(ft.equals(Format.Type.LINE))
            {
                format_reel = new LineFormat(f.getPath());
            }
            else
            {
                throw new IOException();
            }
            format_reel.open(Format.OpenMode.W);
            for(KV kv : kvs)
            {
                format_reel.write(kv);
            }
            format_reel.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    //Retourne une liste de fragments lue dans un fichier
    private Object lireFragment(File f)
    {
        Object ret = null;
        try {
            ret = new ArrayList<KV>();
            Format format_reel;
            if (ft.equals(Format.Type.KV))
            {
                format_reel = new KVFormat(f.getPath());

            }
            else if(ft.equals(Format.Type.LINE))
            {
                format_reel = new LineFormat(f.getPath());
            }
            else
            {
                throw new IOException();
            }
            format_reel.open(Format.OpenMode.R);
            KV lu;
            do {
                lu = format_reel.read();
                if(lu!=null)
                {
                    ((List<KV>)ret).add(new KV(lu.k,lu.v));
                }
            }while(lu!=null);
            format_reel.close();

        } catch (IOException i) {
            i.printStackTrace();
        }
        return ret;
    }

    @Override
    //Indique si un fragment est stocke dans le repertoire de travail
    // a partir du nom du fragment (sans extension)
    public boolean fragmentExiste(String nom) {
        File f = getFichNom(nom);
        if(f!= null)
        {
            return f.exists();
        }
        else
        {
            return false;
        }
    }

    @Override
    //Genere un nom de fichier sans extension inutilisé
    public String getNomNouveauFichier() {
        int num = 0;
        int max =0;
        List<File> existants = getFichInDir();
        for(File f : existants)
        {
            try{
                int v = Integer.parseInt(getFileNameWithoutExtension(f));
                if(v>max)
                {
                    max = v;
                }
            }catch(Exception e)
            {
                e.printStackTrace();
            }
        }
        return Integer.toString(max+1);
    }

    @Override
    // supprime un fragment a partir du nom du fragment (sans extension)
    public void supprimerFragment(String nom) {
        File f = getFichNom(nom);
        if(f!= null)
        {
            f.delete();
        }
    }

    @Override
    public String getDirectory() throws RemoteException {
        return directory;
    }

    @Override
    public String getFormat() throws RemoteException {
        if (ft.equals(Format.Type.KV))
        {
            return "Kv";
        }
        else if(ft.equals(Format.Type.LINE))
        {
            return "Line";
        }
        return null;
    }
}
