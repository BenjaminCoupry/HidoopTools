package ordo;

import java.rmi.RemoteException;

import formats.Format;
import formats.Format.Type;
import map.MapReduce;
import map.Mapper;

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

  @Override
  public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
    m.map(reader, writer);
  }

}