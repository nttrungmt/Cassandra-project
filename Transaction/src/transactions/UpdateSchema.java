package transactions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class UpdateSchema {
	private Cluster cluster;
	private static Session session;

	// function to connect to the cluster
	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		// Get a session from your cluster and store the reference to it.
		session = cluster.connect();
	}

	public void createSchema() {
		// new key space based on D8
		session.execute("CREATE KEYSPACE d8keyspace WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':2};");

		// new key space based on D40
				session.execute("CREATE KEYSPACE d40keyspace WITH replication "
						+ "= {'class':'SimpleStrategy', 'replication_factor':2};");
				
		// new tables
		
	}
	
	public void createDeliveryTable() {
		session.execute(
			      "CREATE TABLE d8keyspace.orderStatus ");
	}
	
	public void createOrderStatusTable() {
		
		session.execute(
				"CREATE TYPE d8keyspace.orderList (" +
		"ol_i_id int," +
		"ol_supply_w_id int," +
        "ol_quantity int," +
        "ol_amount int," +
        "ol_delivery_d timestamp, " +
        ");");
		
		session.execute(
			      "CREATE TABLE d8keyspace.orderStatus (" +
			            "c_id int," +
			            "c_name varchar(34)," +
			            "c_balance int, " + 
			            "o_id int," +
			            "o_entry_d timestamp," +
			            "o_carrier_id int," +
			            "orderList set <frozen <orderList> >," +
			            "PRIMARY KEY (c_id, o_id, o_entry_d)" +
			            ") " + 
			            "WITH CLUSTERING ORDER BY (o_entry_d DESC) ); ");

	}

	public void close() {
		cluster.close();
	}

	public static void main(String[] args) {
		UpdateSchema client = new UpdateSchema();
		client.connect("192.168.8.128");
		client.createSchema();
		
		client.createOrderStatusTable();

		client.close();
	}
}