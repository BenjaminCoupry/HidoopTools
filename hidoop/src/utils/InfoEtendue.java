package utils;

public class InfoEtendue  extends AdresseFrag implements  InfoAdresse{
    public String getAdresseIP() {
        return adresseIP;
    }

    public String getRepertoire() {
        return repertoire;
    }

    String adresseIP;
    String repertoire;
    public InfoEtendue(InfoAdresse i, String adresseIP, String repertoire) {
        super(i.getNomMachine(), i.getNomLocal(), i.getNomFichierComplet());
        this.adresseIP = adresseIP;
        this.repertoire = repertoire;
    }
    public InfoEtendue(String nomMachine, String nomLocal, String nomFichierComplet, String adresseIP, String repertoire) {
        super(nomMachine, nomLocal, nomFichierComplet);
        this.adresseIP = adresseIP;
        this.repertoire = repertoire;
    }
}
