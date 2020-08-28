
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

    // Retrieving all the demography info
    public ResultSet getAllDemographyInfo () throws Exception {
        String sql =  "SELECT * FROM demographyinfo ORDER BY user;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire participants
    public ResultSet getAllQuestionnaireParticipants () throws Exception {
        String sql =  "SELECT * FROM questionnaireparticipants;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire responses info
    public ResultSet getAllQuestionnaireResponses () throws Exception {
        String sql =  "SELECT * FROM questionnaireresponses;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire
    public ResultSet getAllQuestionnaires () throws Exception {
        String sql =  "SELECT * FROM questionnaires;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire templates
    public ResultSet getAllQuestionnaireTemplates () throws Exception {
        String sql =  "SELECT * FROM questionnairetemplates;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the questionnaire
    public ResultSet getAllQuestionnaireWines () throws Exception {
        String sql =  "SELECT * FROM questionnairewines;";
        return stmt.executeQuery(sql);
    }

    // Retrieving all the wine participant assignment
    public ResultSet getAllWineParticipantAssignment () throws Exception {
        String sql =  "SELECT * FROM wineparticipantassignment;";
        return stmt.executeQuery(sql);
    }
}
