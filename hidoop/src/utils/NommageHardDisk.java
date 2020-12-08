package Utils;

import com.sun.istack.internal.Nullable;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class NommageHardDisk extends UnicastRemoteObject implements Nommage{
    String directory;

    public NommageHardDisk(String directory) throws  RemoteException{
        this.directory = directory;
    }


    private static final Pattern ext = Pattern.compile("(?<=.)\\.[^.]+$");

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
                    if(file.getName().contains(".nommage")) {
                        results.add(file);
                    }
                }
            }
            return results;
        }
    }

    @Nullable
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

    private void enregistrerFragments(List<InfoAdresse>frags , File f)
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

    @Override
    public List<InfoAdresse> getAdressesFragments(String nomFichier)throws RemoteException {
        File fich = getFichNom(nomFichier);
        List<InfoAdresse> ret = new ArrayList<>();
        if(fich != null)
        {
            try {
                FileInputStream fileIn = new FileInputStream(fich);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                ret = (List<InfoAdresse>) in.readObject();
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
        }
        return ret;
    }

    @Override
    public void enregistrerAdressesFragments(String nomFichier, List<InfoAdresse> adressesFragments)throws RemoteException {
        List<InfoAdresse> dejaPresent = getAdressesFragments(nomFichier);
        dejaPresent.addAll(adressesFragments);
        supprimerAdressesFragment(nomFichier);
        enregistrerFragments(dejaPresent,new File(nomFichier+".nommage"));
    }

    @Override
    public void enregistrerAdresseFragment(String nomFichier, InfoAdresse adresseFragment)throws RemoteException {
        List<InfoAdresse> dejaPresent = getAdressesFragments(nomFichier);
        dejaPresent.add(adresseFragment);
        supprimerAdressesFragment(nomFichier);
        enregistrerFragments(dejaPresent,new File(nomFichier+".nommage"));
    }

    @Override
    public void supprimerAdressesFragment(String nomFichier) throws RemoteException {
        File f = getFichNom(nomFichier);
        if(f!= null)
        {
            f.delete();
        }
    }
}
