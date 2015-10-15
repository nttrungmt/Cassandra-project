package transactions;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction {
	// variables
	private Session session;
	private String keyspace;
	private int node;

	// constructor
	Transaction(Session session, String keyspace, int node) {
		this.session = session;
		this.keyspace = keyspace;
		this.node = node;
	}

	// New Order Transaction function
	// void newOrder(int w_id, int d_id, int c_id, int m, int[] ol_i_id, int[]
	// ol_supply_w_id, int[] ol_quantity) {
	void newOrder(int w_id, int d_id, int c_id, int m, Set<orderList> orderSet) {
		
		String c_name = null;
		int o_id =0;
		Timestamp o_entry_d = null;
		
		insertOrderStatus(c_id, c_name, o_id, o_entry_d, orderSet);
	}

	// Print new order transaction details
	void printNewOrderDetails() {

	}

	// Payment Transaction function
	void payment(int w_id, int d_id, int c_id, int payment) {

	}

	// Print payment transaction details
	void printPaymentDetails() {

	}

	// Delivery Transaction function
	void delivery(int w_id, int o_carrier_id) {
		int c_balance = 0, o_id =0;
		
		
		
		
		updateOrderStatus(c_balance, o_carrier_id, o_id);
	}

	// Insert table for order status transaction
	void insertOrderStatus(int c_id, String c_name, int o_id,
			Timestamp o_entry_d, Set<orderList> orderList) {
		PreparedStatement statement = session
				.prepare("INSERT INTO "
						+ keyspace
						+ ".oderStatus "
						+ "(c_id, c_name, c_balance, o_id, o_entry_d, o_carrier_id, orderList) "
						+ "VALUES " + "(?, ?, ?, ?, ?, ?, ?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(c_id, c_name, 0, o_id, o_entry_d,
				0, orderList));
	}

	// Update table for order status transaction
	void updateOrderStatus(int c_balance, int o_id, int o_carrier_id) {
		PreparedStatement statement = session
				.prepare("UPDATE "
						+ keyspace
						+ ".oderStatus "
						+ "SET c_balance = ?, o_carrier_id =?  WHERE o_id = ?;)"
						);
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(c_balance, o_carrier_id, o_id));
	}

	// Print order status transaction
	void printOrderStatus(int c_w_id, int c_d_id, int c_id) {
		PreparedStatement statement = session.prepare("SELECT * FROM "
				+ keyspace + ".oderStatus " + "WHERE c_id = ? LIMIT 1;");
		BoundStatement boundStatement = new BoundStatement(statement);

		ResultSet results = session.execute(boundStatement.bind(c_id));

		for (Row row : results) {
			System.out.println("Customer's Name : " + row.getString("c_name"));
			System.out.println("Balance : " + row.getString("c_balance"));

			// for the last order
			System.out.println("Last Order ID : " + row.getString("o_id"));
			System.out.println("Last Order's Entry : "
					+ row.getString("o_entry_d"));
			System.out.println("Last Order's Carrier Identifier : "
					+ row.getString("o_carrier_id"));

			// For each item in last order
			System.out.println("Order List:");
			System.out
					.println("---------------------------------------------------------------------------------");
			System.out.println(String.format(
					"%-20s\t%-20s\t%-20s\t%-20s\t%-20s", ("ol_i_id"),
					("ol_supply_w_id"), ("ol_quantity"), ("ol_amount"),
					("ol_delivery_d")));

			/*
			 * Set<String> set = new HashSet<String>(); set =
			 * row.getString("orderList"); //populate set for (String s : set) {
			 * System.out.println(s); }
			 */

			System.out.println(String.format(
					"%-20s\t%-20s\t%-20s\t%-20s\t%-20s",
					row.getString("ol_i_id"), row.getString("ol_supply_w_id"),
					row.getString("ol_quantity"), row.getString("ol_amount"),
					row.getString("ol_delivery_d")));

			System.out.println("order list: ");
			System.out.println(row.getSet("orderList", orderList.class));
		}
		System.out.println();
	}

	// Print Stock level transaction
	void stockLevel(int w_id, int d_id, int t, int l) {

	}

	// Update table for stock level transactions
	void updateStockLevel() {

	}

	// Update table for Popular Item transaction
	void popularItem(int w_id, int d_id, int l) {

	}

	// Print popular item transaction details
	void printPopularItem() {

	}

	public void loadData1() {
		PreparedStatement statement = session
				.prepare("INSERT INTO simplex.songs "
						+ "(id, title, album, artist, tags) "
						+ "VALUES (?, ?, ?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);
		Set<String> tags = new HashSet<String>();
		tags.add("jazz");
		tags.add("2013");
		session.execute(boundStatement.bind(
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
				"La Petite Tonkinoise'", "Bye Bye Blackbird'",
				"Joséphine Baker", tags));

		statement = session.prepare("INSERT INTO simplex.playlists "
				+ "(id, song_id, title, album, artist) "
				+ "VALUES (?, ?, ?, ?, ?);");
		boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind(
				UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
				"La Petite Tonkinoise", "Bye Bye Blackbird", "Joséphine Baker"));

	}

}
