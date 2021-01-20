package ordo;

import java.rmi.RemoteException;

import formats.*;
import map.*;

public class Action implements Runnable {

    private WorkerInterface worker;
    private MapReduce mr;
    private Format readerMap;
    private Format writerMap;

    public Action (WorkerInterface w, MapReduce m, Format rm, Format rw) {
        this.worker = w;
        this.mr = m;
        this.readerMap = rm;
        this.writerMap = rw;
    }

    public void run() {
        try {
            //traiter les fragments
            worker.runMap(mr, readerMap, writerMap);
        } catch (RemoteException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}