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

    //elements concurrences
    Object mutex = new Object();

    try {
      //recuperer les adresses des fragments
      List<InfoEtendue> adresses = hdfsu.getAdressesFragments(this.inFname);

      // nom du fichier traite
      String mpfname = "res-" + this.inFname;

      //traiter les fragments
      for (InfoEtendue info : adresses) {
        //recuperer le nom de la machine
        String nomMachine = info.getNomMachine();
        // recuperer le nom du fichier
        String nomReaderMap = info.getRepertoire() + "/" + info.getNomLocal();
        String nomWriterMap = "res-" + info.getNomLocal();
        Format readerMap;
        if (this.inFormat == Type.KV) {
          readerMap = new KVFormatS(nomReaderMap);
        } else {
          readerMap = new LineFormatS(nomReaderMap);
        }
        Format writerMap = new KVFormatS(nomWriterMap);
        CallBack cb = new CallBack();
        //recuperer le worker
        WorkerInterface worker = gWorker.getWorker(nomMachine);
        synchronized (mutex) {
          //traiter les fragments
          System.out.println("JOB : runMap() de " + nomReaderMap + " a " + nomWriterMap);
          worker.runMap(mr, readerMap, writerMap, cb);
          //ajouter les fragments aux systemes
          hdfsu.ajouterFragmentSysteme(mpfname, info.getNomMachine(), );
        }
      }

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

      System.out.println("JOB : reduce() de " + readerRed + " dans " + writerRed);
      readerRed.open(Format.OpenMode.R);
      writerRed.open(Format.OpenMode.W);
      mr.reduce(readerRed, writerRed);
      readerRed.close();
      writerRed.close();
      
    } catch (Exception e) {
      e.printStackTrace();
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