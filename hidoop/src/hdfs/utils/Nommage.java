package hdfs.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface Nommage extends Remote {
    List<InfoAdresse> getAdressesFragments(String nomFichier) throws RemoteException;
    void enregistrerAdressesFragments(String nomFichier, List<InfoAdresse> adressesFragments)throws RemoteException;
    void enregistrerAdresseFragment(String nomFichier, InfoAdresse adressesFragment)throws RemoteException;
    void supprimerAdressesFragment(String nomFichier)throws RemoteException;
}
