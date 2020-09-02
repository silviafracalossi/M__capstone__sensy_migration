
import java.sql.*;

public class H2DataExtractor {
    Statement stmt;

    public H2DataExtractor(Statement stmt) {
      this.stmt = stmt;
    }

    // Retrieving all the accounts
    public ResultSet getAllAccounts () throws Exception {
        String sql = "SELECT * FROM accounts;";
        return stmt.executeQuery(sql);
    }

    // Retrieving the next ID for the accounts
    public int getMaxAccountsIdValue () throws Exception {
        String sql = "SELECT MAX(ID) AS MAX FROM ACCOUNTS;";
        ResultSet result = stmt.executeQuery(sql);
        result.next();
        return result.getInt("MAX");
    }

    // Retrieving all the demography info
    public ResultSet getAllDemographyInfo () throws Exception {
        String sql = "SELECT * FROM demographyinfo ORDER BY user;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire participants
    public ResultSet getAllQuestionnaireParticipants () throws Exception {
        String sql = "SELECT * FROM questionnaireparticipants;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire responses info
    public ResultSet getAllQuestionnaireResponses () throws Exception {
        String sql = "SELECT * FROM questionnaireresponses;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire
    public ResultSet getAllQuestionnaires () throws Exception {
        String sql = "SELECT *," +
                "CASE WHEN questionnaire IS NULL\n" +
                "        THEN 'false'\n" +
                "        ELSE 'true'\n" +
                "        END AS true_has_wines" +
                " FROM questionnaires" +
                "   left join (" +
                "                SELECT questionnaire" +
                "                FROM questionnairewines" +
                "                GROUP BY questionnaire" +
                "\t) as wines on questionnaires.id = wines.questionnaire;";
        return stmt.executeQuery(sql);
    }

    // Retrieving the next ID for the questionnaires
    public int getMaxQuestionnaireIdValue () throws Exception {
        String sql = "SELECT MAX(ID) AS MAX FROM QUESTIONNAIRES;";
        ResultSet result = stmt.executeQuery(sql);
        result.next();
        return result.getInt("MAX");
    }

    // Retrieving all the questionnaire templates
    public ResultSet getAllQuestionnaireTemplates () throws Exception {
        String sql = "SELECT * FROM questionnairetemplates;";
        return stmt.executeQuery(sql);
    }

    // Retrieving the next ID for the questionnaire templates
    public int getMaxQuestionnaireTemplatesIdValue () throws Exception {
        String sql = "SELECT MAX(ID) AS MAX FROM QUESTIONNAIRETEMPLATES;";
        ResultSet result = stmt.executeQuery(sql);
        result.next();
        return result.getInt("MAX");
    }

    // Retrieving all the questionnaire
    public ResultSet getAllQuestionnaireWines () throws Exception {
        String sql = "SELECT * FROM questionnairewines;";
        return stmt.executeQuery(sql);
    }

    // Retrieving the next ID for the wines
    public int getMaxQuestionnaireWinesIdValue () throws Exception {
        String sql = "SELECT MAX(ID) AS MAX FROM QUESTIONNAIREWINES;";
        ResultSet result = stmt.executeQuery(sql);
        result.next();
        return result.getInt("MAX");
    }

    // Retrieving all the wine participant assignment
    public ResultSet getAllWineParticipantAssignment () throws Exception {
        String sql = "SELECT * FROM wineparticipantassignment;";
        return stmt.executeQuery(sql);
    }

    // Retrieving the next code for the panelist
    public int getMaxPanelistCodeValue () throws Exception {
        String sql = "call NEXT VALUE FOR PANELIST_CODE_SEQUENCE;";
        ResultSet result = stmt.executeQuery(sql);
        result.next();
        return result.getInt(1);
    }
}
