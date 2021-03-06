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
		session.execute("CREATE KEYSPACE IF NOT EXISTS d8keyspace WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};");

		// new key space based on D40
		session.execute("CREATE KEYSPACE IF NOT EXISTS d40keyspace WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};");

	}

	// Create different types for different tables
	private void createTypes() {
		session.execute("create type d8keyspace.orderitemlist ("
				+ "ol_i_id int," + "i_name text," + "ol_supply_w_id int,"
				+ "ol_quantity double," + "ol_amount double,"
				+ "s_quantity double," + ");");

		session.execute("create type d8keyspace.cname (" + "c_first text,"
				+ "c_middle text," + "c_last text," + ");");

		session.execute("create type d8keyspace.address (" + "street1 text,"
				+ "street2 text," + "city text," + "state text," + "zip text,"
				+ ");");

		session.execute("create type d8keyspace.orderdeliverylist ("
				+ "ol_i_id int," + "i_name text," + "ol_supply_w_id int,"
				+ "ol_quantity double," + "ol_amount double,"
				+ "s_quantity double," + "ol_delivery_d timestamp );");

	}

	// New order transaction table
	private void createNewOrderTable() {

		session.execute("CREATE TABLE IF NOT EXISTS d8keyspace.neworder ("
				+ "w_id int," + "d_id int," + "c_id int," + "o_id int,"
				+ "c_credit text," + "c_discount double,"
				+ "c_name frozen<cname>," + "d_next_o_id int,"
				+ "d_tax double," + "i_id int," + "i_price double,"
				+ "new_order_items list<frozen<orderitemlist>>,"
				+ "o_entry_d timestamp," + "s_order_cnt int,"
				+ "s_remote_cnt int," + "s_ytd double," + "w_tax double,"
				+ "PRIMARY KEY ((w_id, d_id, c_id), o_id)"
				+ ") WITH CLUSTERING ORDER BY (o_id ASC);");

	}

	// Payment table
	private void createPaymentTable() {
		session.execute("CREATE TABLE IF NOT EXISTS d8keyspace.payment ("
				+ "w_id int," + "c_id int," + "d_id int,"
				+ "c_name frozen<cname>," + "c_address frozen<address>,"
				+ "c_phone text," + "c_since timestamp," + "c_credit text,"
				+ "c_credit_lim double," + "c_discount double,"
				+ "c_balance double," + "w_address frozen<address>,"
				+ "d_address frozen<address>,"
				+ "PRIMARY KEY ((w_id, d_id, c_id)));");

	}

	// create order status table
	public void createOrderStatusTable() {

		session.execute("CREATE TABLE IF NOT EXISTS d8keyspace.orderStatus ("
				+ "c_id int," + "w_id int," + "d_id int," + "o_id int,"
				+ "c_name frozen<cname>," + "c_balance double, "
				+ "o_entry_d timestamp," + "o_carrier_id int,"
				+ "orderList list <frozen <orderdeliverylist> >,"
				+ "PRIMARY KEY ((c_id, d_id,w_id), o_id)" + ") "
				+ "WITH CLUSTERING ORDER BY (o_id DESC); ");

	}

	// itemStock table
	private void createItemStockTable() {

		session.execute("create table IF NOT EXISTS d8keyspace.itemStock ("
				+ "d_id int," + "w_id int," + "o_id int,"
				+ "o_entry_d timestamp," + "c_name frozen<cname>,"
				+ "stocklist list<frozen<orderitemlist>>,"
				+ "PRIMARY KEY ((d_id, w_id),o_id)" + ")"
				+ "WITH CLUSTERING ORDER BY (o_id DESC);");

	}

	public void close() {
		cluster.close();
	}

	public static void main(String[] args) {
		UpdateSchema client = new UpdateSchema();
		client.connect("127.0.0.1");
		client.createSchema();
		client.createTypes();
		client.createNewOrderTable();
		client.createPaymentTable();
		client.createOrderStatusTable();
		client.createItemStockTable();

		client.close();
	}

}
