package ordo;

import java.rmi.RemoteException;

import formats.*;
import formats.Format.*;
import map.*;
import utils.*;
import hdfs.*;
import java.util.*;


public class Job implements JobInterfaceX, Worker {

  private Type inFormat;
  private Type outFormat;
  private String outFname;
  private String inFname;

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
    HDFSUtils hdfsu = new HDFSUtils("../../config/adresses.txt");

    try {
      //récupérer les adresses des fragments
      List<InfoEtendue> adresses = hdfsu.getAdressesFragments(this.inFname);

      // nom du fichier traite
      String mpfname = "res-" + this.inFname;

      //traiter les fragments
      for (InfoEtendue info : adresses) {
        String nomReaderMap = info.getNomLocal();
        String nomWriterMap = "res-" + nomReaderMap;
        Format readerMap;
        if (this.inFormat == Type.KV) {
          readerMap = new KVFormatS(this.inFname);
        } else {
          readerMap = new LineFormatS(nomReaderMap);
        }
        Format writerMap = new KVFormatS(nomWriterMap);
        CallBack cb = new CallBack();
        //créer les fragments traites
        runMap(mr, readerMap, writerMap, cb);
        //ajouter les fragments aux systemes
        hdfsu.ajouterFragmentSysteme(info.getNomMachine(), nomWriterMap, mpfname);
      }

      //lire le nouveau fichier creer à partir des nouveaux fragments
      String nomReaderRed = "loc-" + mpfname;
      HdfsClient.HdfsRead(mpfname, nomReaderRed);

      //reduire le fichier que l'on vient de lire
      Format readerRed = new KVFormatS(nomReaderRed);
      Format writerRed;
      if (this.outFormat == Type.KV) {
        writerRed = new KVFormatS(this.outFname);
      } else {
        writerRed = new LineFormatS(this.outFname);
      }

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
  public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
    reader.open(Format.OpenMode.R);
    writer.open(Format.OpenMode.W);
    m.map(reader, writer);
    reader.close();
    writer.close();
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