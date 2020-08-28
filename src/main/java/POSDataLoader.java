
import javax.xml.transform.Result;
import java.sql.*;

public class POSDataLoader {
    Statement stmt;

    // Dropping the schema every time to avoid conflicts with old records //TODO remove this
    public POSDataLoader(Statement stmt) throws Exception {
        this.stmt = stmt;
        String drop_db_sql = "DROP SCHEMA IF EXISTS public CASCADE;";
        stmt.executeUpdate(drop_db_sql);
        System.out.println("[DROP] Schema");

        String create_db_sql = "CREATE SCHEMA public;";
        stmt.executeUpdate(create_db_sql);
        System.out.println("[CREATION] Schema");
    }


    // ========================================ACCOUNTS=================================================================
    // Migrating the accounts data from H2 to PostgreSQL
    public Boolean migrateAccounts(ResultSet h2_accounts) throws Exception {
        System.out.println("\n== Accounts ==");
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

        String accounts_table_sql = "CREATE TABLE accounts " +
                "(" +
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
                "    CONSTRAINT accounts_pk PRIMARY KEY (id_account)," +
                "    CONSTRAINT accounts_code_unique UNIQUE (code)," +
                "    CONSTRAINT accounts_email_unique UNIQUE (email)" +
                ")";

        if (stmt.executeUpdate(accounts_id_sequence_sql) == 0) {
            System.out.println("[CREATION] Sequence: accounts PK");
            if (stmt.executeUpdate(accounts_table_sql) == 0) {
                System.out.println("[CREATION] Table: accounts");
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
                    System.out.println("[ERROR] Problem executing the following script: \n"+final_query);
                } else {
                    rows_inserted++;
                }
                count++;
            }

            // Insertion result and return
            System.out.println("[INSERTION] Accounts inserted: " +rows_inserted+ "/" +count);
            return (rows_inserted == count);
        }
        return false;
    }


    // =====================================DEMOGRAPHYINFO===============================================================

    // Migrating the demographic data from H2 to PostgreSQL
    public Boolean migrateDemographicInfo(ResultSet h2_demographyinfo) throws Exception {
        System.out.println("\n== Demographic Info ==");
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
                "    CONSTRAINT demographic_info_pk PRIMARY KEY (id_account),\n" +
                "    CONSTRAINT demographic_info_id_account_fk FOREIGN KEY (id_account)\n" +
                "        REFERENCES accounts (id_account) MATCH SIMPLE\n" +
                "        ON UPDATE CASCADE\n" +
                "        ON DELETE CASCADE\n" +
                ")";

        if (stmt.executeUpdate(demographic_info_table_sql) == 0) {
            System.out.println("[CREATION] Table: demographic_info");
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
                        System.out.println("[ERROR] Problem executing the following script: \n"+final_query);
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
                System.out.println("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
        }

        // Insertion result and return
        System.out.println("[INSERTION] Demographic Information inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }

    // =====================================QUESTIONNAIRETEMPLATES===============================================================

    // Migrating the templates data from H2 to PostgreSQL
    public Boolean migrateTemplates(ResultSet h2_questionnairetemplates) throws Exception {
        System.out.println("\n== Templates ==");
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
                "    id_template bigint NOT NULL DEFAULT nextval('templates_id_seq'::regclass),\n" +
                "    id_creator_account bigint,\n" +
                "    name character varying(128) COLLATE pg_catalog.\"default\" NOT NULL,\n" +
                "    time_created  timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "    template_xml bytea NOT NULL,\n" +
                "    CONSTRAINT templates_pk PRIMARY KEY (id_template),\n" +
                "    CONSTRAINT templates_id_creator_account_fk FOREIGN KEY (id_creator_account)\n" +
                "        REFERENCES accounts (id_account) MATCH SIMPLE\n" +
                "        ON UPDATE CASCADE\n" +
                "        ON DELETE SET NULL\n" +
                ")";

        if (stmt.executeUpdate(templates_id_sequence_sql) == 0) {
            System.out.println("[CREATION] Sequence: templates PK");
            if (stmt.executeUpdate(templates_table_sql) == 0) {
                System.out.println("[CREATION] Table: templates");
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
                System.out.println("[ERROR] Problem executing the following script: \n"+final_query);
            } else {
                rows_inserted++;
            }
            count++;
        }

        // Insertion result and return
        System.out.println("[INSERTION] Templates inserted: " +rows_inserted+ "/" +count);
        return (rows_inserted == count);
    }


}
