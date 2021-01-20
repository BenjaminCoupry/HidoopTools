package ordo;

import java.io.Serializable;

public class CallBack implements Serializable {

    private static final long serialVersionUID = 1L;

    private int compteur;

    public CallBack () {
        compteur = 0;
    }

    public void incr () {
        synchronized (Job.mutex) {
            ++compteur;
        }
    }

    public int get () {
        return compteur;
    }
}
