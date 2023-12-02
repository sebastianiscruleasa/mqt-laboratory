package mqt.laboratory.ex5v2;

public class Club {
    private String nume;
    private String tara;
    private String oras;
    private int anulInfiintarii;

    public Club(String nume, String tara, String oras, int anulInfiintarii) {
        this.nume = nume;
        this.tara = tara;
        this.oras = oras;
        this.anulInfiintarii = anulInfiintarii;
    }

    public String getNume() {
        return nume;
    }

    public String getTara() {
        return tara;
    }

    public String getOras() {
        return oras;
    }

    public int getAnulInfiintarii() {
        return anulInfiintarii;
    }

    public void setNume(String nume) {
        this.nume = nume;
    }

    public void setTara(String tara) {
        this.tara = tara;
    }

    public void setOras(String oras) {
        this.oras = oras;
    }

    public void setAnulInfiintarii(int anulInfiintarii) {
        this.anulInfiintarii = anulInfiintarii;
    }
}
