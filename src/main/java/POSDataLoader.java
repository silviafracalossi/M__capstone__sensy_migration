
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
            String insert_sql = "INSERT INTO public.accounts (" +
                    "id_account, first_name, last_name, email, password, account_type, code, " +
                    "time_registered, time_last_login, guest_login_code)" +
                    " VALUES ";

            String row_insertion_sql = "";
            Boolean first = true;
            int count = 0;

            // Iterating through all the accounts retrieved by H2
            while (h2_accounts.next()) {

                // Checking first to add delimiter between different rows
                if (first) {
                    first = false;
                } else {
                    row_insertion_sql +=", ";
                }

                // Creating the VALUES part for the current account
                row_insertion_sql += "(";
                row_insertion_sql += "'" + h2_accounts.getInt("id") + "', ";
                row_insertion_sql += "'" + h2_accounts.getString("name") + "', ";
                row_insertion_sql += "'', ";
                row_insertion_sql += "'" + h2_accounts.getString("email") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getBytes("password") + "', ";
                row_insertion_sql += "'" + h2_accounts.getInt("account_type") + "', ";
                row_insertion_sql += "'" + h2_accounts.getInt("code") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getTimestamp("time_registered") + "', ";
                row_insertion_sql += "'" + h2_accounts.getTimestamp("time_last_login") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getByte("guest_login_code") + "'";
                row_insertion_sql += ")";
                row_insertion_sql = row_insertion_sql.replace("''", "NULL");

                count++;
            }

            // Final sql fixes and execution of the script
            insert_sql +=row_insertion_sql + ";";
            int rows_affected = stmt.executeUpdate(insert_sql);

            // Insertion result and return
            System.out.println("[INSERTION] Accounts inserted: " +rows_affected+ "/" +count);
            return (rows_affected == count);
        }
        return false;
    }

    // Retrieving all the accounts
    public ResultSet getAllAccounts () throws Exception {
        System.out.println("Getting all accounts from PosgreSQL database...");
        String sql =  "SELECT * FROM accounts;";
        return stmt.executeQuery(sql);
    }


    // =====================================DEMOGRAPHYINFO===============================================================

    // Migrating the accounts data from H2 to PostgreSQL
    public Boolean migrateDemographicInfo(ResultSet h2_demographyinfo) throws Exception {
        System.out.println("\n== Demographic Info ==");
        if (createDemographicInfoTable()) {

            // Preparing the information
            String prepared_information = transformDemographicInfo(h2_demographyinfo);

            // Splitting between count (0) and insertion script (1)
            String[] splitted_info = prepared_information.split("/", 2);

            // Inserting the rows and printing the result
            int rows_affected = insertDemographicInfo(splitted_info[1]);
            System.out.println("[INSERTION] Demographic Info inserted: " +rows_affected+ "/" +splitted_info[0]);
            return (rows_affected == Integer.parseInt(splitted_info[0]));
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
                "        REFERENCES public.accounts (id_account) MATCH SIMPLE\n" +
                "        ON UPDATE CASCADE\n" +
                "        ON DELETE CASCADE\n" +
                ")";

        if (stmt.executeUpdate(demographic_info_table_sql) == 0) {
            System.out.println("[CREATION] Table: demographic_info");
            return true;
        }
        return false;
    }

    // Preparing the data for the insertion in the new structure
    public String transformDemographicInfo(ResultSet h2_demographyinfo) throws SQLException {

        // Iterating through all the demographyic info retrieved from H2
        int prev_id=-1, count=0;
        String[] demographic_information = new String[10];
        String insertion_sql = "";
        Boolean first = true;

        while (h2_demographyinfo.next()) {

            // Getting the information for the current row
            int current_id = h2_demographyinfo.getInt("user");
            String question_id = h2_demographyinfo.getString("question_id");
            String response = h2_demographyinfo.getString("response");

            // Preparing the insertion script
            if (current_id != prev_id) {
                if (prev_id != -1) {
                    insertion_sql += (insertion_sql.compareTo("") == 0) ? "" : ",";
                    for (int i=0; i<10; i++) {
                        insertion_sql += (i==0) ? "(" : ", ";
                        insertion_sql += "'" +demographic_information[i] +"'";
                    }
                    insertion_sql += ")";
                }

                // Starting with a new user
                count++;
                prev_id = current_id;
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
            insertion_sql += (insertion_sql.compareTo("") == 0) ? "" : ", ";
            for (int i=0; i<10; i++) {
                insertion_sql += (i==0) ? "(" : ", ";
                insertion_sql += "'" +demographic_information[i] +"'";
            }
            insertion_sql += ")";
        }

        // Returning both the count and the prepared insertion script
        insertion_sql = insertion_sql.replace("'null'", "NULL");
        return count + "/" + insertion_sql + ";";
    }

    // Inserting the demographic information extracted from H2 database
    public int insertDemographicInfo (String insertion_part) throws SQLException {

        if (insertion_part != "") {
            String insert_sql = "INSERT INTO demographic_info (" +
                    "id_account, gender, phone_number, birth_year, home_country, home_region, education, " +
                    "smoking_detail, food_intolerance_detail, sulfite_intolerance)" +
                    " VALUES " +
                    insertion_part;

            // Insertion result and return
            int rows_affected = stmt.executeUpdate(insert_sql);

            return rows_affected;
        }

        return 0;
    }


}
