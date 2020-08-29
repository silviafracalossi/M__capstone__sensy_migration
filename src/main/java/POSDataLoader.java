
import javax.xml.transform.Result;
import java.sql.*;
import java.util.logging.Logger;

public class POSDataLoader {
    Statement stmt;
    Logger logger;

    // Dropping the schema every time to avoid conflicts with old records //TODO remove this
    public POSDataLoader(Statement stmt, Logger logger) throws Exception {
        this.stmt = stmt;
        this.logger = logger;
        String drop_db_sql = "DROP SCHEMA IF EXISTS public CASCADE;";
        stmt.executeUpdate(drop_db_sql);
        logger.info("[DROP] Schema");

        String create_db_sql = "CREATE SCHEMA public;";
        stmt.executeUpdate(create_db_sql);
        logger.info("[CREATION] Schema");
    }


    // ========================================ACCOUNTS=================================================================
    // Migrating the accounts data from H2 to PostgreSQL
    public Boolean migrateAccounts(ResultSet h2_accounts) throws Exception {
        logger.info("\n== Accounts ==");
        if (createAccountsTable()) {
            return insertAccounts(h2_accounts);
        }
        return false;
    }

    // Creating the Accounts table, including the PK sequence
    public Boolean createAccountsTable() throws Exception {

        String accounts_id_sequence_sql = "CREATE SEQUENCE accounts_id_seq" +
                "    INCREMENT 1" +
                "    START 1" +
                "    MINVALUE 1" +
                "    MAXVALUE 9223372036854775807" +
                "    CACHE 1;";

        String accounts_table_sql = "CREATE TABLE accounts (" +
                "    id_account bigint NOT NULL DEFAULT nextval('accounts_id_seq'::regclass)," +
                "    first_name character varying(128) COLLATE pg_catalog.\"default\"," +
                "    last_name character varying(128) COLLATE pg_catalog.\"default\"," +
                "    email character varying(128) COLLATE pg_catalog.\"default\"," +
                "    password bytea NOT NULL," +
                "    account_type integer NOT NULL," +
                "    code integer," +
                "    time_registered timestamp with time zone NOT NULL," +
                "    time_last_login timestamp with time zone NOT NULL," +
                "    guest_login_code bytea," +
                "    CONSTRAINT accounts__pk PRIMARY KEY (id_account)," +
                "    CONSTRAINT accounts__code__unique UNIQUE (code)," +
                "    CONSTRAINT accounts__email__unique UNIQUE (email)" +
                ")";

        if (stmt.executeUpdate(accounts_id_sequence_sql) == 0) {
            logger.info("[CREATION] Sequence: accounts PK");
            if (stmt.executeUpdate(accounts_table_sql) == 0) {
                logger.info("[CREATION] Table: accounts");
                return true;
            }
        }
        return false;
    }

    // Inserting the accounts provided by the H2 database
    public Boolean insertAccounts(ResultSet h2_accounts) throws Exception {
        if (h2_accounts != null) {
            String insert_sql = "INSERT INTO accounts (" +
                    "id_account, first_name, last_name, email, password, account_type, code, " +
                    "time_registered, time_last_login, guest_login_code)" +
                    " VALUES ";

            String row_insertion_sql = "";
            int count = 0, rows_inserted = 0;

            // Iterating through all the accounts retrieved by H2
            while (h2_accounts.next()) {

                // Creating the VALUES part for the current account
                row_insertion_sql = "(";
                row_insertion_sql += "'" + h2_accounts.getInt("id") + "', ";
                row_insertion_sql += "'" + h2_accounts.getString("name") + "', ";
                row_insertion_sql += "'', ";
                row_insertion_sql += "'" + h2_accounts.getString("email") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getBytes("password") + "', ";
                row_insertion_sql += "'" + h2_accounts.getInt("account_type") + "', ";
                row_insertion_sql += "'" + h2_accounts.getInt("code") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getTimestamp("time_registered") + "', ";
                row_insertion_sql += "'" + h2_accounts.getTimestamp("time_last_login") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getBytes("guest_login_code") + "'";
                row_insertion_sql += ")";
                row_insertion_sql = row_insertion_sql.replace("''", "NULL");

                // Insert the account into database
                String final_query = insert_sql+row_insertion_sql+";";
                if (stmt.executeUpdate(final_query) != 1) {
                    logger.info("[ERROR] Problem executing the following script: \n"+final_query);
                } else {
                    rows_inserted++;
                }
                count++;
            }

            // Insertion result and return
            logger.info("[INSERTION] Accounts inserted: " +rows_inserted+ "/" +count);
            return (rows_inserted == count);
        }
        return false;
    }


    // =====================================DEMOGRAPHYINFO==============================================================

    // Migrating the demographic data from H2 to PostgreSQL
    public Boolean migrateDemographicInfo(ResultSet h2_demographyinfo) throws Exception {
        logger.info("\n== Demographic Info ==");
        if (createDemographicInfoTable()) {
            return insertDemographicInfo(h2_demographyinfo);
        }
        return false;
    }

    // Creating the Demographic Info table
    public Boolean createDemographicInfoTable() throws Exception {

        String demographic_info_table_sql = "CREATE TABLE demographic_info (" +
                "    id_account bigint NOT NULL,\n" +
                "    gender character varying(6) NOT NULL, \n"   +
                "    phone_number character varying(15) NOT NULL, \n"   +
                "    birth_year int NOT NULL, \n"   +
                "    home_country character varying(60) NOT NULL, \n"   +
                "    home_region character varying(60) NOT NULL, \n"   +
                "    education character varying(20) NOT NULL, \n"   +
                "    smoking_detail character varying(100) DEFAULT NULL, \n"   +
                "    food_intolerance_detail character varying(20) DEFAULT NULL, \n"   +
                "    sulfite_intolerance boolean DEFAULT 'false', \n"   +
                "    CONSTRAINT demographic_info__pk PRIMARY KEY (id_account),\n" +
                "    CONSTRAINT demographic_info__id_account__fk FOREIGN KEY (id_account)\n" +
                "        REFERENCES accounts (id_account) MATCH SIMPLE\n" +
                "        ON UPDATE CASCADE\n" +
                "        ON DELETE CASCADE\n" +
                ")";

        if (stmt.executeUpdate(demographic_info_table_sql) == 0) {
            logger.info("[CREATION] Table: demographic_info");
            return true;
        }
        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertDemographicInfo(ResultSet h2_demographyinfo) throws SQLException {

        String insert_sql = "INSERT INTO demographic_info (" +
                "id_account, gender, phone_number, birth_year, home_country, home_region, education, " +
                "smoking_detail, food_intolerance_detail, sulfite_intolerance)" +
                " VALUES ";

        // Iterating through all the demographic info retrieved from H2
        int prev_id=-1, count=0, rows_inserted=0;
        String[] demographic_information = new String[10];
        String row_insertion = "";

        while (h2_demographyinfo.next()) {

            // Getting the information for the current row
            int current_id = h2_demographyinfo.getInt("user");
            String question_id = h2_demographyinfo.getString("question_id");
            String response = h2_demographyinfo.getString("response");

            // Preparing the insertion script
            if (current_id != prev_id) {

                // If it is not the first row of the H2 ResultSet
                if (prev_id != -1) {

                    // Retrieving the sorted information
                    for (int i=0; i<10; i++) {
                        row_insertion += (i==0) ? "(" : ", ";
                        row_insertion += "'" +demographic_information[i] +"'";
                    }
                    row_insertion += ")";

                    // Preparing and executing the complete insertion script
                    String final_query = insert_sql + row_insertion.replace("'null'", "NULL") + ";";
                    if (stmt.executeUpdate(final_query) != 1) {
                        logger.info("[ERROR] Problem executing the following script: \n"+final_query);
                    } else {
                        rows_inserted++;
                    }
                }

                // Starting with a new user
                count++;
                prev_id = current_id;
                row_insertion = "";
                demographic_information[0] = current_id + "";
            }

            // Defining where to store the new information
            int response_location;
            switch (question_id) {
                case "gender":
                    response_location = 1;
                    break;
                case "phone-number":
                    response_location = 2;
                    break;
                case "year-of-birth":
                    response_location = 3;
                    break;
                case "home-country":
                    response_location = 4;
                    break;
                case "home-region":
                    response_location = 5;
                    break;
                case "education":
                    response_location = 6;
                    break;
                case "smoking-detail-yes":
                    response_location = 7;
                    break;
                case "food-intolerance-detail-yes":
                    response_location = 8;
                    break;
                case "sulfite-intolerance":
                    response_location = 9;
                    break;
                default:
                    response_location = 10;
                    break;
            }

            // Storing information sorted in array
            if (response_location != 10) {
                demographic_information[response_location] = response+"";
            }
        }

        // Saving last user
        if (count != 0) {

            // Retrieving the sorted information
            for (int i=0; i<10; i++) {
                row_insertion += (i==0) ? "(" : ", ";
                row_insertion += "'" +demographic_information[i] +"'";
            }
            row_insertion += ")";

            // Preparing and executing the complete insertion script
            String final_query = insert_sql + row_insertion.replace("'null'", "NULL") + ";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
        }

        // Insertion result and return
        logger.info("[INSERTION] Demographic Information inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }

    // =====================================QUESTIONNAIRETEMPLATES======================================================

    // Migrating the templates data from H2 to PostgreSQL
    public Boolean migrateTemplates(ResultSet h2_questionnairetemplates) throws Exception {
        logger.info("\n== Templates ==");
        if (createTemplatesTable()) {
            return insertTemplates(h2_questionnairetemplates);
        }
        return false;
    }

    // Creating the Templates table, including the PK sequence
    public Boolean createTemplatesTable() throws Exception {

        String templates_id_sequence_sql = "CREATE SEQUENCE templates_id_seq" +
                "    INCREMENT 1" +
                "    START 1" +
                "    MINVALUE 1" +
                "    MAXVALUE 9223372036854775807" +
                "    CACHE 1;";

        String templates_table_sql = "CREATE TABLE templates (" +
                "    id_template bigint NOT NULL DEFAULT nextval('templates_id_seq'::regclass)," +
                "    id_creator_account bigint," +
                "    name character varying(128) COLLATE pg_catalog.\"default\" NOT NULL," +
                "    time_created timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "    template_xml bytea NOT NULL," +
                "    CONSTRAINT templates__pk PRIMARY KEY (id_template)," +
                "    CONSTRAINT templates__id_creator_account__fk FOREIGN KEY (id_creator_account)" +
                "        REFERENCES accounts (id_account) MATCH SIMPLE" +
                "        ON UPDATE CASCADE\n" +
                "        ON DELETE SET NULL\n" +
                ")";

        if (stmt.executeUpdate(templates_id_sequence_sql) == 0) {
            logger.info("[CREATION] Sequence: templates PK");
            if (stmt.executeUpdate(templates_table_sql) == 0) {
                logger.info("[CREATION] Table: templates");
                return true;
            }
        }
        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertTemplates(ResultSet h2_questionnairetemplates) throws SQLException {

        String insert_sql = "INSERT INTO templates (" +
                "id_template, id_creator_account, name, time_created, template_xml" +
                ") VALUES ";

        // Iterating through all the templates retrieved from H2
        String row_insertion_sql = "";
        int count = 0, rows_inserted = 0;
        while (h2_questionnairetemplates.next()) {

            // Creating the VALUES part for the current template
            row_insertion_sql = "(";
            row_insertion_sql += "'" + h2_questionnairetemplates.getInt("id") + "', ";
            row_insertion_sql += "'" + h2_questionnairetemplates.getInt("created_by") + "', ";
            row_insertion_sql += "'" + h2_questionnairetemplates.getString("name") + "', ";
            row_insertion_sql += "'" + h2_questionnairetemplates.getTimestamp("time_created") + "', ";
            row_insertion_sql+= "'" + h2_questionnairetemplates.getBytes("template_xml") + "'";
            row_insertion_sql += ")";
            row_insertion_sql = row_insertion_sql.replace("''", "NULL");

            // Insert the template into database
            String final_query = insert_sql+row_insertion_sql+";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        logger.info("[INSERTION] Templates inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }

    // =====================================QUESTIONNAIRES==============================================================

    // Migrating the questionnaires data from H2 to PostgreSQL
    public Boolean migrateQuestionnaires(ResultSet h2_questionnaires) throws Exception {
        logger.info("\n== Questionnaires ==");
        if (createQuestionnairesTable()) {
            return insertQuestionnaires(h2_questionnaires);
        }
        return false;
    }

    // Creating the Questionnaire table, including the PK sequence
    public Boolean createQuestionnairesTable() throws Exception {

        String questionnaires_id_sequence_sql = "CREATE SEQUENCE questionnaires_id_seq" +
                "    INCREMENT 1" +
                "    START 1" +
                "    MINVALUE 1" +
                "    MAXVALUE 9223372036854775807" +
                "    CACHE 1;";

        String questionnaires_table_sql = "CREATE TABLE questionnaires (" +
                "    id_questionnaire bigint NOT NULL DEFAULT nextval('questionnaires_id_seq'::regclass)," +
                "    id_template bigint NOT NULL," +
                "    id_creator_account bigint," +
                "    name character varying(128) COLLATE pg_catalog.\"default\" NOT NULL," +
                "    time_created timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "    state integer NOT NULL DEFAULT 0," +
                "    has_wines boolean NOT NULL DEFAULT 'false'," +
                "    CONSTRAINT questionnaires__pk PRIMARY KEY (id_questionnaire)," +
                "    CONSTRAINT questionnaires__id_creator_account__fk FOREIGN KEY (id_creator_account)" +
                "        REFERENCES accounts (id_account) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE SET NULL," +
                "    CONSTRAINT questionnaires__id_template FOREIGN KEY (id_template)" +
                "        REFERENCES templates (id_template) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE" +
                ")";

        if (stmt.executeUpdate(questionnaires_id_sequence_sql) == 0) {
            logger.info("[CREATION] Sequence: questionnaires PK");
            if (stmt.executeUpdate(questionnaires_table_sql) == 0) {
                logger.info("[CREATION] Table: questionnaires");
                return true;
            }
        }
        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertQuestionnaires(ResultSet h2_questionnaires) throws SQLException {

        String insert_sql = "INSERT INTO questionnaires (" +
                "id_questionnaire, id_template, id_creator_account, name, time_created, state, has_wines" +
                ") VALUES ";

        // Iterating through all the questionnaires retrieved from H2
        String row_insertion_sql = "";
        int count = 0, rows_inserted = 0;
        while (h2_questionnaires.next()) {

            // Creating the VALUES part for the current template
            row_insertion_sql = "(";
            row_insertion_sql += "'" + h2_questionnaires.getInt("id") + "', ";
            row_insertion_sql += "'" + h2_questionnaires.getInt("template") + "', ";
            row_insertion_sql += "'" + h2_questionnaires.getInt("created_by") + "', ";
            row_insertion_sql += "'" + h2_questionnaires.getString("name") + "', ";
            row_insertion_sql += "'" + h2_questionnaires.getTimestamp("time_created") + "', ";
            row_insertion_sql += "'" + h2_questionnaires.getInt("state") + "', ";

            // changing the attribute from has_no_wines to has_wines
            row_insertion_sql += "'" +
                    ((h2_questionnaires.getBoolean("has_no_wines")) ? "false":"true")
                    + "'";
            row_insertion_sql += ")";
            row_insertion_sql = row_insertion_sql.replace("''", "NULL");

            // Insert the questionnaire into database
            String final_query = insert_sql+row_insertion_sql+";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        logger.info("[INSERTION] Questionnaires inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }


    // =====================================WINES==============================================================

    // Migrating the wines data from H2 to PostgreSQL
    public Boolean migrateWines(ResultSet h2_questionnairewines) throws Exception {
        logger.info("\n== Wines ==");
        if (createWinesTable()) {
            return insertWines(h2_questionnairewines);
        }
        return false;
    }

    // Creating the Wines table, including the sequence and index
    public Boolean createWinesTable() throws Exception {

        String wines_id_sequence_sql = "CREATE SEQUENCE wines_id_seq" +
                "    INCREMENT 1" +
                "    START 1" +
                "    MINVALUE 1" +
                "    MAXVALUE 9223372036854775807" +
                "    CACHE 1;";

        String wines_table_sql = "CREATE TABLE wines (" +
                "    id_wine bigint NOT NULL DEFAULT nextval('wines_id_seq'::regclass)," +
                "    id_questionnaire bigint NOT NULL," +
                "    name character varying(128) COLLATE pg_catalog.\"default\" NOT NULL," +
                "    code integer NOT NULL," +
                "    CONSTRAINT wines__pk PRIMARY KEY (id_wine)," +
                "    CONSTRAINT wines__id_questionnaire__fk FOREIGN KEY (id_questionnaire)" +
                "        REFERENCES questionnaires (id_questionnaire) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE" +
                ")";

        String wines_index = "CREATE INDEX wines__id_questionnaire__index\n" +
                "    ON wines USING btree\n" +
                "    (id_questionnaire ASC NULLS LAST)\n" +
                "    TABLESPACE pg_default;";

        if (stmt.executeUpdate(wines_id_sequence_sql) == 0) {
            logger.info("[CREATION] Sequence: wines PK");
            if (stmt.executeUpdate(wines_table_sql) == 0) {
                logger.info("[CREATION] Table: wines");
                if (stmt.executeUpdate(wines_index) == 0) {
                    logger.info("[CREATION] Index: wines");
                    return true;
                }
            }
        }
        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertWines(ResultSet h2_questionnairewines) throws SQLException {

        String insert_sql = "INSERT INTO wines (" +
                "id_wine, id_questionnaire, name, code" +
                ") VALUES ";

        // Iterating through all the wines retrieved from H2
        String row_insertion_sql = "";
        int count = 0, rows_inserted = 0;
        while (h2_questionnairewines.next()) {

            // Creating the VALUES part for the current template
            row_insertion_sql = "(";
            row_insertion_sql += "'" + h2_questionnairewines.getInt("id") + "', ";
            row_insertion_sql += "'" + h2_questionnairewines.getInt("questionnaire") + "', ";
            row_insertion_sql += "'" + h2_questionnairewines.getString("name") + "', ";
            row_insertion_sql += "'" + h2_questionnairewines.getInt("code") + "'";
            row_insertion_sql += ")";
            row_insertion_sql = row_insertion_sql.replace("''", "NULL");

            // Insert the wines into database
            String final_query = insert_sql+row_insertion_sql+";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        logger.info("[INSERTION] Wines inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }


    // =====================================PARTICIPATES===============================================================

    // Migrating the participates data from H2 to PostgreSQL
    public Boolean migrateParticipates(ResultSet h2_questionnaireparticipants) throws Exception {
        logger.info("\n== Participates ==");
        if (createParticipatesTable()) {
            return insertParticipates(h2_questionnaireparticipants);
        }
        return false;
    }

    // Creating the participates table
    public Boolean createParticipatesTable() throws Exception {

        String participates_table_sql = "CREATE TABLE participates (" +
                "    id_participant_account bigint NOT NULL," +
                "    id_questionnaire bigint NOT NULL," +
                "    state integer NOT NULL DEFAULT 0," +
                "    curr_wine_no integer NOT NULL DEFAULT 0," +
                "    curr_section_no integer NOT NULL DEFAULT 0," +
                "    time_section_started timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "    CONSTRAINT participates__pk PRIMARY KEY (id_participant_account, id_questionnaire)," +
                "    CONSTRAINT participates__id_participant__fk FOREIGN KEY (id_participant_account)" +
                "        REFERENCES accounts (id_account) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE," +
                "    CONSTRAINT participates__id_questionnaire__fk FOREIGN KEY (id_questionnaire)" +
                "        REFERENCES questionnaires (id_questionnaire) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE" +
                ")";

        if (stmt.executeUpdate(participates_table_sql) == 0) {
            logger.info("[CREATION] Table: participates");
            return true;
        }
        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertParticipates(ResultSet h2_questionnaireparticipants) throws SQLException {

        String insert_sql = "INSERT INTO participates (" +
                "id_participant_account, id_questionnaire, state, curr_wine_no, " +
                "curr_section_no, time_section_started" +
                ") VALUES ";

        // Iterating through all the participates retrieved from H2
        String row_insertion_sql = "";
        int count = 0, rows_inserted = 0;
        while (h2_questionnaireparticipants.next()) {

            // Creating the VALUES part for the current template
            row_insertion_sql = "(";
            row_insertion_sql += "'" + h2_questionnaireparticipants.getInt("participant") + "', ";
            row_insertion_sql += "'" + h2_questionnaireparticipants.getInt("questionnaire") + "', ";
            row_insertion_sql += "'" + h2_questionnaireparticipants.getInt("state") + "', ";
            row_insertion_sql += "'" + h2_questionnaireparticipants.getInt("wine_index") + "', ";
            row_insertion_sql += "'" + h2_questionnaireparticipants.getInt("section") + "', ";
            row_insertion_sql += "'" + h2_questionnaireparticipants.getTimestamp("section_started_at");
            row_insertion_sql += "')";
            row_insertion_sql = row_insertion_sql.replace("''", "NULL");

            // Insert the participates into database
            String final_query = insert_sql+row_insertion_sql+";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        logger.info("[INSERTION] Questionnaires inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }


    // =====================================WINES ANSWERING ORDER==============================================================

    // Migrating the wines answering order data from H2 to PostgreSQL
    public Boolean migrateWinesAnswOrder(ResultSet h2_wineparticipantassignment) throws Exception {
        logger.info("\n== Wines Answering Order ==");
        if (createWinesAnswOrderTable()) {
            return insertWinesAnswOrder(h2_wineparticipantassignment);
        }
        return false;
    }

    // Creating the Wines Answering Order table
    public Boolean createWinesAnswOrderTable() throws Exception {

        String wines_answ_order_table_sql = "CREATE TABLE wines_answ_order (" +
                "    id_questionnaire bigint NOT NULL," +
                "    id_participant_account bigint NOT NULL," +
                "    id_wine bigint NOT NULL," +
                "    position integer NOT NULL," +
                "    CONSTRAINT wines_answ_order__pk PRIMARY KEY (id_questionnaire, id_participant_account, position)," +
                "    CONSTRAINT wines_answ_order__id_questionnaire_id_wine_id_participant__unique UNIQUE" +
                "        (id_questionnaire, id_participant_account, id_wine)," +
                "    CONSTRAINT wines_answ_order__id_participant_account_id_questionnaire__fk FOREIGN KEY (id_participant_account, id_questionnaire)" +
                "        REFERENCES participates (id_participant_account, id_questionnaire) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE," +
                "    CONSTRAINT wines_answ_order__id_questionnaire__fk FOREIGN KEY (id_questionnaire)" +
                "        REFERENCES questionnaires (id_questionnaire) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE," +
                "    CONSTRAINT wines_answ_order__id_wine__fk FOREIGN KEY (id_wine)" +
                "        REFERENCES wines (id_wine) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE" +
                ")";

        if (stmt.executeUpdate(wines_answ_order_table_sql) == 0) {
            logger.info("[CREATION] Table: wines_answ_order");
            return true;
        }

        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertWinesAnswOrder(ResultSet h2_wineparticipantassignment) throws SQLException {

        String insert_sql = "INSERT INTO wines_answ_order (" +
                "id_questionnaire, id_participant_account, id_wine, position" +
                ") VALUES ";

        // Iterating through all the wines retrieved from H2
        String row_insertion_sql = "";
        int count = 0, rows_inserted = 0;
        while (h2_wineparticipantassignment.next()) {

            // Creating the VALUES part for the current template
            row_insertion_sql = "(";
            row_insertion_sql += "'" + h2_wineparticipantassignment.getInt("questionnaire") + "', ";
            row_insertion_sql += "'" + h2_wineparticipantassignment.getInt("participant") + "', ";
            row_insertion_sql += "'" + h2_wineparticipantassignment.getInt("wine") + "', ";
            row_insertion_sql += "'" + h2_wineparticipantassignment.getInt("order") + "'";
            row_insertion_sql += ")";
            row_insertion_sql = row_insertion_sql.replace("''", "NULL");

            // Insert the wines answering order into database
            String final_query = insert_sql+row_insertion_sql+";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        logger.info("[INSERTION] Wines Answering Order inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }


    // =====================================RESPONSES==============================================================

    // Migrating the responses data from H2 to PostgreSQL
    public Boolean migrateResponses(ResultSet h2_questionnaireresponses) throws Exception {
        logger.info("\n== Responses ==");
        if (createReponsesTable()) {
            return insertResponses(h2_questionnaireresponses);
        }
        return false;
    }

    // Creating the Responses table
    public Boolean createReponsesTable() throws Exception {

        String responses_table_sql = "CREATE TABLE responses (" +
                "    id_questionnaire bigint NOT NULL," +
                "    id_participant_account bigint NOT NULL," +
                "    id_wine bigint NOT NULL," +
                "    question character varying(64) COLLATE pg_catalog.\"default\" NOT NULL," +
                "    response character varying(500) COLLATE pg_catalog.\"default\" NOT NULL," +
                "    CONSTRAINT responses__pk PRIMARY KEY (id_participant_account, id_questionnaire, id_wine, question)," +
                "    CONSTRAINT responses__id_participant_account_id_questionnaire__fk FOREIGN KEY (id_participant_account, id_questionnaire)\n" +
                "        REFERENCES participates (id_participant_account, id_questionnaire) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE RESTRICT," +
                "    CONSTRAINT responses__id_questionnaire__fk FOREIGN KEY (id_questionnaire)" +
                "        REFERENCES questionnaires (id_questionnaire) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE," +
                "    CONSTRAINT responses__id_wine__fk FOREIGN KEY (id_wine)" +
                "        REFERENCES wines (id_wine) MATCH SIMPLE" +
                "        ON UPDATE CASCADE" +
                "        ON DELETE CASCADE" +
                ")";

        if (stmt.executeUpdate(responses_table_sql) == 0) {
            logger.info("[CREATION] Table: responses");
            return true;
        }

        return false;
    }

    // Preparing and inserting the data extracted from H2 database
    public Boolean insertResponses(ResultSet h2_questionnaireresponses) throws SQLException {

        String insert_sql = "INSERT INTO responses (" +
                "id_questionnaire, id_participant_account, id_wine, question, response" +
                ") VALUES ";

        // Iterating through all the responses retrieved from H2
        String row_insertion_sql = "";
        int count = 0, rows_inserted = 0;
        while (h2_questionnaireresponses.next()) {

            // Creating the VALUES part for the current template
            row_insertion_sql = "(";
            row_insertion_sql += "'" + h2_questionnaireresponses.getInt("questionnaire") + "', ";
            row_insertion_sql += "'" + h2_questionnaireresponses.getInt("participant") + "', ";
            row_insertion_sql += "'" + h2_questionnaireresponses.getInt("wine") + "', ";
            row_insertion_sql += "'" + h2_questionnaireresponses.getString("question_id") + "',";
            row_insertion_sql += "'" + h2_questionnaireresponses.getString("response") + "'";
            row_insertion_sql += ")";
            row_insertion_sql = row_insertion_sql.replace("''", "NULL");

            // Insert the responses into database
            String final_query = insert_sql+row_insertion_sql+";";
            if (stmt.executeUpdate(final_query) != 1) {
                logger.info("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        logger.info("[INSERTION] Responses inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }

}
