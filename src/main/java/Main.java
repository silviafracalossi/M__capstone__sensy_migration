
import java.sql.*;

public class Main {

   // H2 Database Configurations          // TODO: change DB_URL, user and password
   static final String H2_DB_URL = "jdbc:h2:D:\\Uni\\repositories\\vincent\\build\\cache\\-database\\vincent";
   static final String H2_USER = "";
   static final String H2_PASS = "";

   // Postgres Database Configurations    // TODO: change DB_URL, user and password
   static final String POS_DB_URL = "jdbc:postgresql://localhost/";
   static final String POS_DB_NAME = "sensy_migration";
   static final String POS_DB_USER = "postgres";
   static final String POS_PASS = "silvia";

   static H2DataExtractor h2de;
   static POSDataLoader posdl;


   public static void main(String[] args) {

      // Defining the connection and statement variables
      Connection h2_conn = null;
      Statement h2_stmt = null;

      Connection pos_conn = null;
      Statement pos_stmt = null;

      try {

         // Opening a connection to the H2 database (DATA ORIGIN)
         System.out.println("Connecting to the H2 database...");
         h2_conn = DriverManager.getConnection(H2_DB_URL,H2_USER,H2_PASS);
         h2_stmt = h2_conn.createStatement();

         // Opening a connection to the postgreSQL database (DATA DESTINATION)
         System.out.println("Connecting to the PostgreSQLs database...");
         String pos_complete_url = POS_DB_URL + POS_DB_NAME + "?user=" + POS_DB_USER +"&password=" + POS_PASS;
         pos_conn = DriverManager.getConnection(pos_complete_url);
         pos_stmt = pos_conn.createStatement();

         // Creating the objects for methods calling
         h2de = new H2DataExtractor(h2_stmt);
         posdl = new POSDataLoader(pos_stmt);

         // Migration
         Boolean accounts_migration = posdl.migrateAccounts(h2de.getAllAccounts());
         Boolean demographic_info_migration = posdl.migrateDemographicInfo(h2de.getAllDemographyInfo());
         Boolean templates_migration = posdl.migrateTemplates(h2de.getAllQuestionnaireTemplates());
         Boolean questionnaires_migration = posdl.migrateQuestionnaires(h2de.getAllQuestionnaires());

         // Printing the results
         System.out.println("\n-----------------------------------");
         System.out.println("\t\tMIGRATION - SUM UP\t\t");
         System.out.println("Accounts\t\t\t|\t" + ((accounts_migration) ? "successful" : "fail."));
         System.out.println("Demographic Info\t|\t" + ((demographic_info_migration) ? "successful" : "fail."));
         System.out.println("Questionnaires\t\t|\t" + ((questionnaires_migration) ? "successful" : "fail."));
         System.out.println("Templates\t\t\t|\t" + ((templates_migration) ? "successful" : "fail."));
         System.out.println("-----------------------------------");

         // Closing connections and statements
         h2_stmt.close();
         h2_conn.close();

      } catch(Exception e) {
         e.printStackTrace();
      } finally {
         try{
            if(h2_stmt!=null) h2_stmt.close();
            if(pos_stmt!=null) pos_stmt.close();
         } catch(SQLException se2) {
         }
         try {
            if(h2_conn!=null) h2_conn.close();
            if(pos_conn!=null) pos_conn.close();
         } catch(SQLException se){
            se.printStackTrace();
         }
      }
   }


}
