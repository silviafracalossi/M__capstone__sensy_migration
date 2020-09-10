
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {

   // BE CAREFUL!
   static final boolean useServerH2DB = true;
   static final boolean useServerPostgresDB = false;

   static H2DataExtractor h2de;
   static POSDataLoader posdl;

   // ==================H2 configurations====================
   // LOCAL Configurations
   static final String H2_DB_URL = "jdbc:h2:D:\\Uni\\repositories\\vincent\\build\\cache\\-database\\vincent";
   static final String H2_USER = "", H2_PASS = "";

   // SERVER Configurations
   static final String server_H2_DB_URL = "jdbc:h2:D:\\Uni\\repositories\\sensy_migration\\database\\server_vincent";
   static final String server_H2_USER = "", server_H2_PASS = "";


   // ==================PostgreSQL configurations===================
   // LOCAL Configurations
   static final String POS_DB_URL = "jdbc:postgresql://localhost/";
   static final String POS_DB_NAME = "sensy_migration";
   static final String POS_DB_USER = "postgres";
   static final String POS_PASS = "silvia";

   // SERVER Configurations
   static final String server_POS_DB_URL = "jdbc:postgresql://sensy.inf.unibz.it/";
   static final String server_POS_DB_NAME = "sensy";
   static String server_POS_DB_USER;
   static String server_POS_PASS;


   public static void main(String[] args) {

      // Creating the logger
      Logger logger = Logger.getLogger("MigrationLog");
      FileHandler fh;

      // Defining the connection and statement variables
      Connection h2_conn = null;
      Statement h2_stmt = null;

      Connection pos_conn = null;
      Statement pos_stmt = null;

      // Loading the credentials to the new postgresql database
      try {
        File myObj = new File("database/server_postgresql_credentials.txt");
        Scanner myReader = new Scanner(myObj);
         server_POS_DB_USER = myReader.nextLine();
         server_POS_PASS = myReader.nextLine();
        myReader.close();
      } catch (FileNotFoundException e) {
        System.out.println("Please, remember to create the database credentials file (see README)");
        e.printStackTrace();
      }

      try {

         // Instantiating logger
         fh = new FileHandler("migration.log");
         logger.addHandler(fh);
         SimpleFormatter formatter = new SimpleFormatter();
         fh.setFormatter(formatter);

         // Opening a connection to the H2 database (DATA ORIGIN)
         logger.info("Connecting to the H2 database...");
         if (useServerH2DB) {
            h2_conn = DriverManager.getConnection(server_H2_DB_URL, server_H2_USER, server_H2_PASS);
         } else {
            h2_conn = DriverManager.getConnection(H2_DB_URL, H2_USER, H2_PASS);
         }
         h2_stmt = h2_conn.createStatement();

         // Opening a connection to the postgreSQL database (DATA DESTINATION)
         logger.info("Connecting to the PostgreSQLs database...");
         String pos_complete_url;
         if (useServerPostgresDB) {
            pos_complete_url = server_POS_DB_URL + server_POS_DB_NAME +
                    "?user=" + server_POS_DB_USER +"&password=" + server_POS_PASS;
         } else {
            pos_complete_url = POS_DB_URL + POS_DB_NAME + "?user=" + POS_DB_USER +"&password=" + POS_PASS;
         }

         pos_conn = DriverManager.getConnection(pos_complete_url);
         pos_stmt = pos_conn.createStatement();

         // Creating the objects for methods calling
         h2de = new H2DataExtractor(h2_stmt);
         posdl = new POSDataLoader(pos_conn, pos_stmt, logger);

         // Migration
         Boolean accounts_migration = posdl.migrateAccounts(
                 h2de.getMaxAccountsIdValue(),
                 h2de.getAllAccounts()
         );
         Boolean demographic_info_migration = posdl.migrateDemographicInfo(h2de.getAllDemographyInfo());
         Boolean templates_migration = posdl.migrateTemplates(
                 h2de.getMaxQuestionnaireTemplatesIdValue(),
                 h2de.getAllQuestionnaireTemplates()
         );
         Boolean questionnaires_migration = posdl.migrateQuestionnaires(
                 h2de.getMaxQuestionnaireIdValue(),
                 h2de.getAllQuestionnaires()
         );
         Boolean wines_migration = posdl.migrateWines(
                 h2de.getMaxQuestionnaireWinesIdValue(),
                 h2de.getAllQuestionnaireWines()
         );
         Boolean participates_migration = posdl.migrateParticipates(h2de.getAllQuestionnaireParticipants());
         Boolean wines_answ_order_migration = posdl.migrateWinesAnswOrder(h2de.getAllWineParticipantAssignment());
         Boolean responses_migration = posdl.migrateResponses(h2de.getAllQuestionnaireResponses());

         // Creating the panelist sequence
         logger.info("\n== Panelist Code Sequence ==");
         logger.info("Creation resulted " + ((posdl.createPanelistCodeSequence(
                 h2de.getMaxPanelistCodeValue())) ? "successful" : "fail."));

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
             se2.printStackTrace();
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
