package ordo;

import map.Mapper;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import formats.Format;

public class Worker extends UnicastRemoteObject implements WorkerInterface {

  private static final long serialVersionUID = 1L;

  public Worker() throws RemoteException {}

  public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
    reader.open(Format.OpenMode.R);
    writer.open(Format.OpenMode.W);
    m.map(reader, writer);
    reader.close();
    writer.close();
  }
}
