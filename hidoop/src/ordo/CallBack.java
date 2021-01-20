package ordo;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class CallBack implements Serializable {

    private static final long serialVersionUID = 1L;

    private AtomicInteger compteur;

    public CallBack () {
        new AtomicInteger();
    }

    public void incr () {
        this.compteur.incrementAndGet();
    }

    public int get () {
        return this.compteur.get();
    }
}
