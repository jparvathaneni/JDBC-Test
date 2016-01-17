package com.jags;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;

/**
 * Created by JParvathaneni on 1/16/16.
 */
public class ConnectionExample {

    private static final Log LOG = LogFactory.getLog(ConnectionExample.class);

    private static String dbURL = "jdbc:h2:~/Development/JDBC-Test/test";

    public static void main(String[] args) throws SQLException {
        Connection dbCon = createConnection();

        createTable(dbCon);

        insertData(dbCon);

        readData(dbCon);

        LOG.debug(dbCon);

        dbCon.close();
    }

    private static void readData(Connection dbCon) throws SQLException {
        String sql = "select personid, lastname, firstname, address, city from persons";
        Statement stmt = dbCon.createStatement();
        ResultSet resultSet = stmt.executeQuery(sql);
        while(resultSet.next()){
            int personid = resultSet.getInt("personid");
            String firstname = resultSet.getString("firstname");
            LOG.debug(personid + "  - " + firstname);
        }
        resultSet.close();
        stmt.close();
    }

    private static void insertData(Connection dbCon) throws SQLException {
        String SQL = "INSERT INTO PERSONS VALUES (1, 'PARVATHANENI', 'JAGADEESH', '10895 CORAL SHORES DR', 'JACKSONVILLE')";
        Statement statement = dbCon.createStatement();
        statement.executeUpdate(SQL);
        statement.close();

        String pSQL = "INSERT INTO PERSONS VALUES (?, ?, ?, ?, ?)";
        PreparedStatement stmt1 = dbCon.prepareStatement(pSQL);
        stmt1.setInt(1, 2);
        stmt1.setString(2, "Kantamneni");
        stmt1.setString(3, "Teja");
        stmt1.setString(4, "Gate Pky");
        stmt1.setString(5, "Jacksonville");
        stmt1.execute();

        stmt1.setInt(1, 3);
        stmt1.setString(2, "Kantamneni");
        stmt1.setString(3, "Keya");
        stmt1.setString(4, "Gate Pky");
        stmt1.setString(5, "Jacksonville");

        stmt1.execute();

        stmt1.close();
    }

    private static void createTable(Connection connection) throws SQLException {
        String sql = "CREATE TABLE Persons" +
                "(" +
                "PersonID int," +
                "LastName varchar(255)," +
                "FirstName varchar(255)," +
                "Address varchar(255)," +
                "City varchar(255)" +
                ")";

        Statement statement = connection.createStatement();
        statement.execute(sql);

        statement.close();

    }

    private static Connection createConnection() {
        try {
            Class.forName("org.h2.Driver");
            return DriverManager.getConnection(dbURL, "sa", "");
        } catch (Exception except) {
            LOG.error("exception creating db connection", except);
        }
        return null;
    }

}
