package ordo;

public class GetterWorker {

  private Worker worker;

  public GetterWorker(String configuration) {
    this.worker = null;
    File config = new File(configuration);
    try {
        FileInputStream fis=new FileInputStream(config);
        Scanner sc=new Scanner(fis);    //file to be scanned
        //returns true if there is another line to read
        while(sc.hasNextLine())
        {
            String ligne = sc.nextLine();   //returns the line that was skipped
            String[] spt = ligne.split(" ");
            if(spt[0].equals("Nommage")) {
                String nom = spt[1];
                String adresse = spt[2];
                String port = spt[3];
                try {
                    // get the stub of the server object from the rmiregistry
                    Nommage nm  = (Nommage) Naming.lookup(adresse+":"+port+"/serviceNommageFragments");
                    nommage = nm;
                    System.out.println("Serveur de nommage "+nom+" trouvé !");
                } catch (Exception exc) {
                    System.out.println(exc.getMessage());
                }
                break;
            }
        }
        sc.close();     //closes the scanner

    }
  }
}