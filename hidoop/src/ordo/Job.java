package ordo;

import formats.Format.Type;
import map.MapReduce;

public class Job implements JobInterfaceX {

  @Override
  public void setInputFormat(Type ft) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setInputFname(String fname) {
    // TODO Auto-generated method stub

  }

  @Override
  public void startJob(MapReduce mr) {
    // TODO Auto-generated method stub

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