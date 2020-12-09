package ordo;

import java.rmi.RemoteException;

import formats.Format.*;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.*;
import utils.*;
import hdfs.*;
import java.util.*;


public class Job implements JobInterfaceX, Worker {

  private Type informat;
  private String fname;

  @Override
  public void setInputFormat(Type ft) {
    this.informat = ft;
  }

  @Override
  public void setInputFname(String fname) {
    this.fname = fname;
  }

  @Override
  public void startJob(MapReduce mr) {
    //récupérer les stubs
    HDFSUtils hdfsu = new HDFSUtils("../../config/nommage.txt");

    try {
      //récupérer les adresses des fragments
      List<InfoEtendue> adresses = hdfsu.getAdressesFragments(this.fname);

      // nom du fichier traite
      String mpfname = "res-" + this.fname;

      //traiter les fragments
      for (InfoEtendue info : adresses) {
        String nomReader = info.getNomLocal();
        String nomWriter = "res-" + info.getNomLocal();
        Format reader = new LineFormat(nomReader);
        Format writer = new KVFormat(nomWriter);
        CallBack cb = new CallBack();
        //créer les fragments traites
        runMap(mr, reader, writer, cb);
        //ajouter les fragments aux systemes
        hdfsu.ajouterFragmentSysteme(info.getNomMachine(), nomWriter, mpfname);
      }

      //lire le nouveau fichier creer à partir des nouveaux fragments
      HdfsClient.HdfsRead(mpfname, "final-" + mpfname);

      //reduire le fichier que l'on vient de lire
      


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
    m.map(reader, writer);
  }

  @Override
  public void setNumberOfReduces(int tasks) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNumberOfMaps(int tasks) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setOutputFormat(Type ft) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setOutputFname(String fname) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setSortComparator(SortComparator sc) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getNumberOfReduces() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getNumberOfMaps() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Type getInputFormat() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Type getOutputFormat() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getInputFname() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getOutputFname() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SortComparator getSortComparator() {
    // TODO Auto-generated method stub
    return null;
  }

}