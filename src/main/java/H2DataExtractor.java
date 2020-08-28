
import java.sql.*;

public class H2DataExtractor {
    Statement stmt;

    public H2DataExtractor(Statement stmt) {
      this.stmt = stmt;
    }

    // Retrieving all the accounts
    public ResultSet getAllAccounts () throws Exception {
        String sql =  "SELECT * FROM accounts;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the accounts
    public ResultSet getAllDemographyInfo () throws Exception {
        String sql =  "SELECT * FROM demographyinfo ORDER BY user;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the accounts
    public ResultSet getAllQuestionnaireTemplates () throws Exception {
        String sql =  "SELECT * FROM questionnairetemplates;";
        return stmt.executeQuery(sql);
    }
}
