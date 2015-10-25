import java.util.Iterator;
import java.util.TreeSet;

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
	private PreparedStatement payment_w_up;
	private PreparedStatement payment_d_up;
	private PreparedStatement payment_c_up;
	private PreparedStatement payment_cm_sel;
	private PreparedStatement payment_c_sel;
	private PreparedStatement del_o_sel;
	private PreparedStatement del_o_up;
	private PreparedStatement del_c_up;
	private PreparedStatement orderst_o_sel;
	private PreparedStatement orderst_c_sel;
	private PreparedStatement stock_d_sel;
	private PreparedStatement stock_o_sel;
	private PreparedStatement stock_s_sel;
	private PreparedStatement popular_o_sel;
	private PreparedStatement popular_s_sel;

	// constructor
	Transaction(Session session, String keyspace, int node) {
		this.session = session;
		this.keyspace = keyspace;
		this.node = node;

		// Payment Prepared Statements
		payment_w_up = session.prepare("UPDATE " + keyspace + ".warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?;");
		payment_d_up = session.prepare("UPDATE " + keyspace + ".district SET d_ytd = d_ytd + ? WHERE w_id = ? and d_id = ?;");
		payment_c_up = session.prepare("UPDATE " + keyspace + ".customer SET c_balance = c_balance + ?, c_ytd_payment = c_ytd_payment+ ?, c_payment_cnt = c_payment_cnt + 1 WHERE w_id = ? and d_id = ? and c_id = ?;");
		payment_cm_sel = session.prepare("SELECT c_name, c_addr, c_phone, c_since, c_credit, c_credit_lim, c_discount, w_addr, d_addr FROM " + keyspace + ".customermaster WHERE w_id = ? and d_id = ? and c_id = ?;");
		payment_c_sel = session.prepare("SELECT c_balance FROM " + keyspace + ".customer WHERE w_id = ? and d_id = ? and c_id = ?;");
		
		// Delivery Prepared Statements
		del_o_sel = session.prepare("SELECT o_id,c_id,ol_id,ol_amount FROM " + keyspace + ".orders WHERE w_id = ? and d_id =? and o_carrier_id = ?;");
		del_o_up = session.prepare("UPDATE " + keyspace + ".orders SET o_carrier_id = ?, ol_delivery_d = dateOf(now()) WHERE w_id =? and d_id = ? and o_id =? and ol_id = ?;");
		del_c_up = session.prepare("UPDATE " + keyspace + ".customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt+1 WHERE w_id = ? and d_id = ? and c_id = ?;");
		
		//Order Status Prepared Statements
		orderst_o_sel = session.prepare("SELECT c_name, o_id, o_entry_d, o_carrier_id, i_id, ol_supply_w_id, ol_qty, ol_amount, ol_delivery_d FROM " + keyspace + ".orders WHERE w_id = ? and d_id = ? and c_id = ?;");
		orderst_c_sel = session.prepare("SELECT c_balance FROM " + keyspace + ".customer WHERE w_id = ? and d_id = ? and c_id = ?;");
		
		// stock level Prepared Statements
		stock_d_sel = session.prepare("SELECT d_next_oid FROM " + keyspace + ".district WHERE w_id = ? AND d_id = ? ;");
		stock_o_sel = session.prepare("SELECT i_id FROM " + keyspace + ".orders WHERE w_id = ? AND d_id = ? and o_id >=?;");
		//stock_s_sel = session.prepare("");
		
		// Popular Item Prepared Statements
		popular_o_sel = session.prepare("SELECT o_id, ol_id, ol_qty, o_entry_d, c_name, i_name, ol_qty FROM " + keyspace + ".orders WHERE w_id = ? AND d_id = ? and o_id >=?;");
		//popular_s_sel = session.prepare("");
	}

	// New Order Transaction function
	void newOrder(int w_id, int d_id, int c_id, int m) {
			/*Set<orderDeliveryList> orderSet) {

		String c_name = null;
		int o_id = 0;
		Timestamp o_entry_d = null;
		// dateOf(now()) function in call to get the current timestamp

		System.out.println("New order transaction!");
		System.out.println("w_id:" + w_id + ", d_id:" + d_id + ", c_id:" + c_id
				+ ", m:" + m);

		// populate set
		
		 * for (orderList s : orderSet) { System.out.println("ol_i_id: " +
		 * s.get_ol_i_id() + ", ol_supply_w_id:" + s.get_ol_supply_w_id() +
		 * ", ol_quantity" + s.get_ol_quantity() + ", ol_amount:" +
		 * s.get_ol_amount() + ", ol_delivery_d:" + s.get_ol_delivery_d()); }
		 */
		// insertOrderStatus(c_id, c_name, o_id, o_entry_d, orderSet);
	}


	// Payment Transaction function
	void payment(int w_id, int d_id, int c_id, double pay) {
		System.out.println("Payment transaction!");
		Long payment = new Long((long) (pay*100));
		System.out.println("w_id:" + w_id + ", d_id:" + d_id + ", c_id:" + c_id
				+ ", payment:" + payment);
		BoundStatement bound_w_up = new BoundStatement(payment_w_up);
		session.execute(bound_w_up.bind(payment, w_id));
		
		BoundStatement bound_d_up = new BoundStatement(payment_d_up);
		session.execute(bound_d_up.bind(payment, w_id, d_id));
		
		BoundStatement bound_c_up = new BoundStatement(payment_c_up);
		session.execute(bound_c_up.bind(payment, payment, w_id, d_id, c_id));
		
		BoundStatement bound_cm_sel = new BoundStatement(payment_cm_sel);
		ResultSet results = session.execute(bound_cm_sel.bind(w_id, d_id, c_id));
		System.out.println(results.all());
		
		BoundStatement bound_c_sel = new BoundStatement(payment_c_sel);
		results = session.execute(bound_c_sel.bind(w_id, d_id, c_id));
		System.out.println(results.all());
		System.out.println(pay);

	}


	// Delivery Transaction function
	void delivery(int w_id, int carrier_id) {

		System.out.println("Delivery transaction!");
		System.out.println("w_id:" + w_id + ", o_carrier_id:" + carrier_id);
		for(int i =1; i<= 10 ; i++) {
			BoundStatement bound_w_up = new BoundStatement(del_o_sel);
			ResultSet results = session.execute(bound_w_up.bind(w_id, i, carrier_id));

			//Set to store values
			/*Set<Integer> oidSet = new HashSet<Integer>();
			while (it.hasNext()) {
				Row row = it.next();
				oidSet.add(row.getInt("o_id"));
			}
			TreeSet<Integer> sortedSet = new TreeSet<Integer>(oidSet);*/
			
			TreeSet<Integer> sortedSet = new TreeSet<Integer>();
			
			for (Row row : results) {
				sortedSet.add(row.getInt("o_id"));
			}
			if(!(sortedSet.isEmpty())) {
				int o_id = sortedSet.first();
			
				int c_id = 0;
				double B = 0.0;
			
				BoundStatement bound_o_up = new BoundStatement(del_o_up);
				Iterator<Row> it = results.iterator();
				while (it.hasNext())  {
					Row row = it.next();
					if(row.getInt("o_id") == o_id) {
						B = B + row.getDouble("ol_amount");
						c_id = row.getInt("c_id");

						session.execute(bound_o_up.bind(carrier_id, w_id, i, o_id, c_id, row.getInt("ol_id")));
					}
				}
				BoundStatement bound_c_up = new BoundStatement(del_c_up);
				session.execute(bound_c_up.bind(B, w_id, i, c_id));
			}
		}

	}


	//  order status transaction
	void orderStatus(int w_id, int d_id, int c_id) {

		System.out.println("order status transaction!");
		System.out.println("w_id:" + w_id + ", d_id:" + d_id + ", c_id:" + c_id
				+ "keyspace:" + keyspace);

		BoundStatement bound_o_sel = new BoundStatement(orderst_o_sel);
		ResultSet results = session.execute(bound_o_sel.bind(w_id, d_id, c_id));
		System.out.println(results.all());
		
		BoundStatement bound_c_sel = new BoundStatement(orderst_c_sel);
		results = session.execute(bound_c_sel.bind(w_id, d_id, c_id));
		System.out.println(results.all());

	}

	// Print Stock level transaction
	void stockLevel(int w_id, int d_id, int t, int l) {
		System.out.println("Stock level transaction!");
		System.out.println("w_id:" + w_id + ", d_id:" + d_id + ", t:" + t
				+ ", l:" + l);
		
		// query to get next_oid
		int next_oid = 0;
		BoundStatement bound_d_sel = new BoundStatement(stock_d_sel);
		ResultSet results = session.execute(bound_d_sel.bind(w_id, d_id));
		for (Row row : results) {
			next_oid = (int)(row.getLong("d_next_oid"));
		}

		// query to get i_id
		int range =  next_oid - l;
		BoundStatement bound_o_sel = new BoundStatement(stock_o_sel);
		results = session.execute(bound_o_sel.bind(w_id, d_id, range));
		
		// query to get s_qty
		String query_s_sel = "SELECT s_qty FROM " + keyspace + ".stocks WHERE w_id= ? and i_id IN (";
		int i = 0;
		for (Row row : results) {
			if(i!=0) {
				query_s_sel = query_s_sel +",";
				i++;
			}
			query_s_sel = query_s_sel + row.getInt("i_id");
		}
		query_s_sel = query_s_sel + ");";
		
		System.out.println(query_s_sel);
		stock_s_sel = session.prepare(query_s_sel);
		BoundStatement bound_s_sel = new BoundStatement(stock_s_sel);
		results = session.execute(bound_s_sel.bind(w_id));
		
		Integer [] qty = new Integer[range];
		i = 0;
		for (Row row : results) {
			qty[i] = row.getInt("s_qty");
			i++;
		}
		int count = 0;
		if(qty[i]<t) count++;
		System.out.println(count);

	}


	// Print popular item transaction details
	void popularItem(int w_id, int d_id, int l) {
		System.out.println("Popular Item transaction!");
		System.out.println("w_id:" + w_id + ", d_id:" + d_id);
		System.out.println("Number of last orders to be examined:" + l);
		
		// query to next_oid
		int next_oid = 0;
		BoundStatement bound_d_sel = new BoundStatement(stock_d_sel);
		ResultSet results = session.execute(bound_d_sel.bind(w_id, d_id));
		for (Row row : results) {
			next_oid = row.getInt("d_next_oid");
		}
		
		//query to get oid for last l orders
		int range = next_oid - l;
		BoundStatement bound_o_sel = new BoundStatement(popular_o_sel);
		results = session.execute(bound_o_sel.bind(w_id, d_id, range));
		
		//storing all oids
		Integer [] oid = new Integer[range];
		int i = 0;
		for (Row row : results) {
			oid[i] = row.getInt("o_id");
			System.out.println("Order number: " + oid[i] + ", entry date and time:" + row.getDate("o_entry_d") + ", Customer name: " + row.getString("c_name"));
			i++;
		}
		
		// processing all orders for all oids
		// o_id, ol_id, ol_qty, o_entry_d, c_name, i_name, ol_qty
		int curOid = 0;
		for(i = 0; i<oid.length; i++) {
			curOid = oid[i];
			Iterator<Row> it = results.iterator();
			while (it.hasNext())  {
				Row row = it.next();
				if(row.getInt("o_id") == curOid) {
					}
			}
			
		}
		

	}

}
