package Utils;

import java.io.Serializable;

public interface InfoAdresse extends Serializable {
    String getNomMachine();
    String getNomLocal();
    String getNomFichierComplet();
    String afficher();
}
