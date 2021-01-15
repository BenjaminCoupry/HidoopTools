package hdfs.utils;

//import com.sun.istack.internal.Nullable;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
@Deprecated
public class GestionnaireFragmentsHardDisk extends UnicastRemoteObject implements GestionnaireFragments{
    String directory;

    private static final Pattern ext = Pattern.compile("(?<=.)\\.[^.]+$");

    public GestionnaireFragmentsHardDisk(String directory)throws RemoteException {
        this.directory = directory;
    }

    public static String getFileNameWithoutExtension(File file) {
        return ext.matcher(file.getName()).replaceAll("");
    }

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
    private File getFichNom(String nomFichier)
    {
        List<File> inDir = getFichInDir();
        for(File nf : inDir)
        {
            if(getFileNameWithoutExtension(nf).equals(nomFichier))
            {
                return nf;
            }
        }
        return null;
    }

    @Override
    public String ecrireFragment(Serializable contenu) {
        String nom = getNomNouveauFichier()+".fragment";
        enregistrerFragment(contenu,new File(nom));
        return nom;
    }

    @Override
    public Object lireFragment(String nom) {
        return lireFragment(getFichNom(nom));
    }

    private void enregistrerFragment(Serializable frags , File f)
    {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(f);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(frags);
            out.close();
            fileOut.close();
            System.out.printf("Serialized data is saved");
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    private Object lireFragment(File f)
    {
        Object ret = null;
        try {
            FileInputStream fileIn = new FileInputStream(f);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            ret = (Object) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return ret;
        } catch (ClassNotFoundException c) {
            System.out.println("class not found");
            c.printStackTrace();
            return ret;
        } 
        return ret;
    }

    @Override
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
        return "HD";
    }
}
