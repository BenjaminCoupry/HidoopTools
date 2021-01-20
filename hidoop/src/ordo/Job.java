package ordo;

import formats.*;
import formats.Format.*;
import map.*;
import hdfs.utils.*;
import hdfs.*;

import java.util.*;

import config.Project;


public class Job implements JobInterfaceX {

  private Type inFormat;
  private Type outFormat;
  private String outFname;
  private String inFname;
  static Thread[] activites;
  static Object mutex = new Object();

  public Job(String inFname, Format.Type inFormat, Format.Type outFormat) {
    this.inFname = inFname;
    this.outFname = "final-" + inFname;
    this.inFormat = inFormat;
    this.outFormat = outFormat;
  }

  @Override
  public void setInputFormat(Type ft) {
    this.inFormat = ft;
  }

  @Override
  public Type getInputFormat() {
    return this.inFormat;
  }


  @Override
  public void setInputFname(String fname) {
    this.inFname = fname;
  }

  @Override
  public String getInputFname() {
    return this.inFname;
  }


  @Override
  public void setOutputFormat(Type ft) {
    this.outFormat = ft;
  }

  @Override
  public Type getOutputFormat() {
    return this.outFormat;
  }


  @Override
  public void setOutputFname(String fname) {
    this.outFname = fname;
  }

  @Override
  public String getOutputFname() {
    return this.outFname;
  }


  @Override
  public void startJob(MapReduce mr) {
    //récupérer les stubs
    HDFSUtils hdfsu = new HDFSUtils(Project.PATH_CONFIG);
    GetterWorker gWorker = new GetterWorker(Project.PATH_CONFIG);
    GetterNommage gNommage = new GetterNommage(Project.PATH_CONFIG);
    Nommage nommage = gNommage.getNommage();

    try {
      //recuperer les adresses des fragments
      List<InfoEtendue> adresses = hdfsu.getAdressesFragments(this.inFname);

      //nom du fichier traite
      String mpfname = "res-" + this.inFname;

      //liste des adresses des nouveaux fragments
      List<InfoAdresse> newAdresses = new LinkedList<InfoAdresse>();

      //tableau d'activites
      int nbActivites = adresses.size();
      int cp = 0;
      activites = new Thread[nbActivites];

      //objet CallBack
      CallBack cb = new CallBack();

      //traiter les fragments
      for (InfoEtendue info : adresses) {
        //recuperer le nom de la machine
        String nomMachine = info.getNomMachine();
        // recuperer le nom du fichier
        String nomReaderMap = info.getRepertoire() + info.getNomLocal();
        String nomWriterMap = info.getRepertoire() + "res-" + info.getNomLocal();
        Format readerMap;
        if (this.inFormat == Type.KV) {
          readerMap = new KVFormatS(nomReaderMap);
        } else {
          readerMap = new LineFormatS(nomReaderMap);
        }
        Format writerMap = new KVFormatS(nomWriterMap);
        //recuperer le worker
        WorkerInterface worker = gWorker.getWorker(nomMachine);
        //traiter les fragments
        System.out.println("JOB : runMap() de " + nomReaderMap + " a " + nomWriterMap);
        Runnable r = new Action(worker, mr, readerMap, writerMap, cb);
        activites[cp] = new Thread(r);
        activites[cp].start();
        //ajouter les nouvelles adresses à la liste
        newAdresses.add(new AdresseFrag(info.getNomMachine(), "res-" + info.getNomLocal(), mpfname));
        ++cp;
      }

      System.out.println("JOB : Attendre la fin du traitement de chaque fragment ...");
      while (cb.get() < nbActivites) { //seulement utile pour l'affichage
        System.out.println("...");
        Thread.sleep(4000);
      }

      finir(nbActivites);

      //ajouter les fragments au serveur de nommage
      nommage.enregistrerAdressesFragments(mpfname, newAdresses);    

      //lire le nouveau fichier creer à partir des nouveaux fragments
      String nomReaderRed = "loc-" + mpfname;
      System.out.println("JOB : Lire " + mpfname + " dans " + nomReaderRed);
      HdfsClient.HdfsRead(mpfname, nomReaderRed);

      //reduire le fichier que l'on vient de lire
      Format readerRed = new KVFormatS(nomReaderRed);
      Format writerRed;
      if (this.outFormat == Type.KV) {
        writerRed = new KVFormatS(this.outFname);
      } else {
        writerRed = new LineFormatS(this.outFname);
      }

      System.out.println("JOB : reduce() de " + nomReaderRed + " dans " + this.outFname);
      readerRed.open(OpenMode.R);
      writerRed.open(OpenMode.W);
      mr.reduce(readerRed, writerRed);
      readerRed.close();
      writerRed.close();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void finir (int nbActivites) throws InterruptedException {
    for (int i = 0; i<nbActivites; ++i) {
      activites[i].join();
    }
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
  public SortComparator getSortComparator() {
    // TODO Auto-generated method stub
    return null;
  }

}