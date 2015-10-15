
import com.datastax.driver.core.*;

public class GettingStarted {
    
    public static void main(String[] args) {
        //create cluster and session instance fields to hold the references.
        //session : to manage the connections to our cluster.
        Cluster cluster;
        Session session;
        
        // Connect to the cluster and keyspace "mykeyspace"
        cluster = Cluster.builder().addContactPoint("192.168.8.128").build();
        session = cluster.connect("testkey");
        
        // Insert one record into the users table
        //session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
        session.execute("INSERT INTO users (user_id, fname, lname) VALUES (1478, 'Austin', 'Jones')");
        
        // Use select to get the user we just entered
        ResultSet results = session.execute("SELECT * FROM users where lname = 'Jones'");
        for (Row row : results) {
            System.out.format("%s %s\n", row.getString("fname"), row.getInt("user_id"));
        }
        System.out.print("next\n\n");
        // Update the same user with a new age
        session.execute("update users set fname = 'abc' where user_id = 1478");
        
        // Select and show the change
        results = session.execute("select * from users where lname='Jones'");
        for (Row row : results) {
            System.out.format("%s %d\n", row.getString("fname"), row.getInt("user_id"));
        }
        System.out.print("next\n\n");
        // Delete the user from the users table
        session.execute("DELETE FROM users WHERE user_id = 1478");
        
        // Show that the user is gone
        results = session.execute("SELECT * FROM users");
        for (Row row : results) {
            System.out.format("%s %d %s\n", row.getString("lname"), row.getInt("user_id"),  row.getString("fname"));
        }
        
        // Clean up the connection by closing it
        cluster.close();
        
    }
}