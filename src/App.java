import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        try {
            // Connexion à la base de données SQLite
            Connection connection = DriverManager.getConnection("jdbc:sqlite:ellipsys_test_db.db3");
            Statement statement = connection.createStatement();

            // Création d'une map pour stocker les correspondances entre les valeurs originales et les entiers
            Map<String, Integer> idMapping = new HashMap<>();
            Map<String, Integer> trfMapping = new HashMap<>();
            Map<String, Integer> tgtTbMapping = new HashMap<>();
            Map<String, Integer> tgtLabMapping = new HashMap<>();
            Map<String, Integer> srcTbMapping = new HashMap<>();
            Map<String, Integer> srcLabMapping = new HashMap<>();
            Map<String, Integer> impactMapping = new HashMap<>();

            // Création des nouvelles tables pour chaque colonne
            statement.execute("CREATE TABLE IF NOT EXISTS oa_trf_src_new (" +
                    "id INTEGER, " +
                    "trf INTEGER, " +
                    "tgtTb INTEGER, " +
                    "tgtLab INTEGER, " +
                    "srcTb INTEGER, " +
                    "srcLab INTEGER, " +
                    "impact INTEGER" +
                    ");");

            // Remplacement des valeurs dans chaque colonne
            ResultSet rs = statement.executeQuery("SELECT * FROM oa_trf_src;");
            while (rs.next()) {
                int id = mapAndRetrieve(rs.getString("id"), idMapping);
                int trf = mapAndRetrieve(rs.getString("trf"), trfMapping);
                int tgtTb = mapAndRetrieve(rs.getString("tgtTb"), tgtTbMapping);
                int tgtLab = mapAndRetrieve(rs.getString("tgtLab"), tgtLabMapping);
                int srcTb = mapAndRetrieve(rs.getString("srcTb"), srcTbMapping);
                int srcLab = mapAndRetrieve(rs.getString("srcLab"), srcLabMapping);
                int impact = mapAndRetrieve(rs.getString("impact"), impactMapping);
                
                // Insérez les données dans la nouvelle table
                statement.execute("INSERT INTO oa_trf_src_new VALUES (" +
                        id + ", " +
                        trf + ", " +
                        tgtTb + ", " +
                        tgtLab + ", " +
                        srcTb + ", " +
                        srcLab + ", " +
                        impact +
                        ");");
                // Affichage des résultats dans le terminal
                System.out.println("id: " + id + ", trf: " + trf + ", tgtTb: " + tgtTb +
                        ", tgtLab: " + tgtLab + ", srcTb: " + srcTb + ", srcLab: " + srcLab +
                        ", impact: " + impact);        
            }

            // Fermer les connexions
            rs.close();
            statement.close();
            connection.close();

            System.out.println("Programme exécuté avec succès.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Fonction pour mapper chaque valeur à un entier et le récupérer
    private static int mapAndRetrieve(String value, Map<String, Integer> mapping) {
        if (value.matches("\\d+")) {
            // Si c'est un entier, le retourner tel quel
            return Integer.parseInt(value);
        } else {
            // Si c'est une chaîne, mapper à un entier unique
            return mapping.computeIfAbsent(value, k -> mapping.size());
        }
    }
}
