
import java.sql.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

      // Creating the logger
      Logger logger = Logger.getLogger("MigrationLog");
      FileHandler fh;

      // Defining the connection and statement variables
      Connection h2_conn = null;
      Statement h2_stmt = null;

      Connection pos_conn = null;
      Statement pos_stmt = null;

      try {

         // Instantiating logger
         fh = new FileHandler("migration.log");
         logger.addHandler(fh);
         SimpleFormatter formatter = new SimpleFormatter();
         fh.setFormatter(formatter);

         // Opening a connection to the H2 database (DATA ORIGIN)
         logger.info("Connecting to the H2 database...");
         h2_conn = DriverManager.getConnection(H2_DB_URL,H2_USER,H2_PASS);
         h2_stmt = h2_conn.createStatement();

         // Opening a connection to the postgreSQL database (DATA DESTINATION)
         logger.info("Connecting to the PostgreSQLs database...");
         String pos_complete_url = POS_DB_URL + POS_DB_NAME + "?user=" + POS_DB_USER +"&password=" + POS_PASS;
         pos_conn = DriverManager.getConnection(pos_complete_url);
         pos_stmt = pos_conn.createStatement();

         // Creating the objects for methods calling
         h2de = new H2DataExtractor(h2_stmt);
         posdl = new POSDataLoader(pos_stmt, logger);

         // Migration
         Boolean accounts_migration = posdl.migrateAccounts(h2de.getAllAccounts());
         Boolean demographic_info_migration = posdl.migrateDemographicInfo(h2de.getAllDemographyInfo());
         Boolean templates_migration = posdl.migrateTemplates(h2de.getAllQuestionnaireTemplates());
         Boolean questionnaires_migration = posdl.migrateQuestionnaires(h2de.getAllQuestionnaires());
         Boolean wines_migration = posdl.migrateWines(h2de.getAllQuestionnaireWines());
         Boolean participates_migration = posdl.migrateParticipates(h2de.getAllQuestionnaireParticipants());
         Boolean wines_answ_order_migration = posdl.migrateWinesAnswOrder(h2de.getAllWineParticipantAssignment());
         Boolean responses_migration = posdl.migrateResponses(h2de.getAllQuestionnaireResponses());

         // Printing the results
         logger.info("\n---------------------------------------");
         logger.info("\t\t MIGRATION - SUM UP");
         logger.info("Accounts\t\t\t\t|\t" + ((accounts_migration) ? "successful" : "fail."));
         logger.info("Demographic Info\t\t\t|\t" + ((demographic_info_migration) ? "successful" : "fail."));
         logger.info("Participates\t\t\t|\t" + ((participates_migration) ? "successful" : "fail."));
         logger.info("Questionnaires\t\t\t|\t" + ((questionnaires_migration) ? "successful" : "fail."));
         logger.info("Responses\t\t\t\t|\t" + ((responses_migration) ? "successful" : "fail."));
         logger.info("Templates\t\t\t\t|\t" + ((templates_migration) ? "successful" : "fail."));
         logger.info("Wines\t\t\t\t|\t" + ((wines_migration) ? "successful" : "fail."));
         logger.info("Wines Answering Order\t\t|\t" + ((wines_answ_order_migration) ? "successful" : "fail."));
         logger.info("---------------------------------------");

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
