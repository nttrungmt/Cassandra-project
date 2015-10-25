import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
		payment_cm_sel = session.prepare("SELECT c_name, c_addr, c_phone, c_since, c_credit, c_credit_lim, c_discount, w_addr, d_addr FROM "
						+ keyspace + ".customermaster WHERE w_id = ? and d_id = ? and c_id = ?;");
		payment_c_sel = session.prepare("SELECT c_balance FROM " + keyspace + ".customer WHERE w_id = ? and d_id = ? and c_id = ?;");

		// Delivery Prepared Statements
		del_o_sel = session.prepare("SELECT o_id,c_id,ol_id,ol_amount FROM " + keyspace + ".orders WHERE w_id = ? and d_id =? and o_carrier_id = 0;");
		del_o_up = session.prepare("UPDATE " + keyspace + ".orders SET o_carrier_id = ?, ol_delivery_d = dateOf(now()) WHERE w_id =? and d_id = ? and o_id =? and ol_id = ?;");
		del_c_up = session.prepare("UPDATE " + keyspace + ".customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt+1 WHERE w_id = ? and d_id = ? and c_id = ?;");

		// Order Status Prepared Statements
		orderst_o_sel = session.prepare("SELECT c_name, o_id, o_entry_d, o_carrier_id, i_id, ol_supply_w_id, ol_qty, ol_amount, ol_delivery_d FROM "
						+ keyspace + ".orders WHERE w_id = ? and d_id = ? and c_id = ?;");
		orderst_c_sel = session.prepare("SELECT c_balance FROM " + keyspace + ".customer WHERE w_id = ? and d_id = ? and c_id = ?;");

		// stock level Prepared Statements
		stock_d_sel = session.prepare("SELECT d_next_oid FROM " + keyspace + ".district WHERE w_id = ? AND d_id = ? ;");
		stock_o_sel = session.prepare("SELECT i_id FROM " + keyspace + ".orders WHERE w_id = ? AND d_id = ? and o_id >=?;");

		// Popular Item Prepared Statements
		popular_o_sel = session.prepare("SELECT o_id, ol_id, i_id, ol_qty, o_entry_d, c_name, i_name, ol_qty FROM "
						+ keyspace + ".orders WHERE w_id = ? AND d_id = ? and o_id >=?;");
	}

	// New Order Transaction function
	void newOrder(int w_id, int d_id, int c_id, int m) {
	}

	// Payment Transaction function
	void payment(int w_id, int d_id, int c_id, double pay) {
		Long payment = new Long((long) (pay*100));
		System.out.println("Payment transaction!");
		System.out.println("Customer's identifier :w_id:" + w_id + ", d_id:" + d_id + ", c_id:" + c_id);
		BoundStatement bound_w_up = new BoundStatement(payment_w_up);
		session.execute(bound_w_up.bind(payment, w_id));
		
		BoundStatement bound_d_up = new BoundStatement(payment_d_up);
		session.execute(bound_d_up.bind(payment, w_id, d_id));
		
		BoundStatement bound_c_up = new BoundStatement(payment_c_up);
		session.execute(bound_c_up.bind(payment, payment, w_id, d_id, c_id));
		
		BoundStatement bound_cm_sel = new BoundStatement(payment_cm_sel);
		ResultSet results = session.execute(bound_cm_sel.bind(w_id, d_id, c_id));
		
		BoundStatement bound_c_sel = new BoundStatement(payment_c_sel);
		ResultSet results1 = session.execute(bound_c_sel.bind(w_id, d_id, c_id));
		
		
		for (Row row : results) {
			System.out.println("Customer's Name : " + row.getString("c_name") + ", Address : " + row.getString("c_addr") + ", Credit" + row.getString("c_credit") + ", Credit_lim: " + row.getDouble("c_credit_lim") + ", Discount:" + row.getDouble("c_discount"));
			for (Row row1 : results1) {
				System.out.println("Customer balance:" + ((double) (row1.getLong("c_balance")))/100);
			}
			System.out.println("Warehouse’s address : " + row.getString("w_addr"));
			System.out.println("District’s address : " + row.getString("d_addr"));
		}
		
		System.out.println("Payment amount:" + pay);

	}

	// Delivery Transaction function
	void delivery(int w_id, int carrier_id) {

		System.out.println("Delivery transaction!");
		System.out.println("w_id:" + w_id + ", o_carrier_id:" + carrier_id);
		for (int i = 1; i <= 10; i++) {
			BoundStatement bound_w_up = new BoundStatement(del_o_sel);
			ResultSet results = session.execute(bound_w_up.bind(w_id, i));
			
			TreeSet<Integer> sortedSet = new TreeSet<Integer>();
			List <String> resList = new ArrayList<String>();
			for (Row row : results) {
				sortedSet.add(row.getInt("o_id"));
				String temp = row.getInt("o_id") + "," + row.getInt("c_id") + "," + row.getInt("ol_id") + "," + row.getDouble("ol_amount");
				resList.add(temp);
			}
			if (!(sortedSet.isEmpty())) {
				int o_id = sortedSet.first();
				int c_id = 0;
				double B = 0.0;
				
				for (int j = 0; j < resList.size(); j++) {
					String[] words = (resList.get(j)).split(",");
					if (o_id == Integer.parseInt(words[0])) {
						c_id = Integer.parseInt(words[1]);
						B = B + Double.parseDouble(words[3]);
						BoundStatement bound_o_up = new BoundStatement(del_o_up);
						session.execute(bound_o_up.bind(carrier_id, w_id, i,
								o_id, Integer.parseInt(words[2])));
					}
				}
				B = B/100;
				BoundStatement bound_c_up = new BoundStatement(del_c_up);
				session.execute(bound_c_up.bind((long)B, w_id, i, c_id));
			}
		}

	}

	// order status transaction
	void orderStatus(int w_id, int d_id, int c_id) {

		System.out.println("order status transaction!");
		System.out.println("w_id:" + w_id + ", d_id:" + d_id + ", c_id:" + c_id);

		BoundStatement bound_o_sel = new BoundStatement(orderst_o_sel);
		ResultSet results = session.execute(bound_o_sel.bind(w_id, d_id, c_id));

		BoundStatement bound_c_sel = new BoundStatement(orderst_c_sel);
		ResultSet results1 = session.execute(bound_c_sel.bind(w_id, d_id, c_id));

		for (Row row : results) {
			System.out.println("Customer's Name : " + row.getString("c_name"));
			for (Row row1 : results1) {
				System.out.println("Balance: " + ((double) (row1.getLong("c_balance")))/100);
			}
			// for the last order
			System.out.println("Customer’s last order : ");
			System.out.println("Order number : "+ row.getInt("o_id") + ", Entry date and time: " + row.getDate("o_entry_d") + ", Carrier identifier: " + row.getInt("o_carrier_id"));
			System.out.println("Item number: " + row.getInt("i_id") + ", Supplying warehouse number : " + row.getInt("ol_supply_w_id") + 
					", Quantity ordered: " + row.getInt("ol_qty") + ", Total price for ordered item: " + row.getDouble("ol_amount") + ", Data and time of delivery:" + row.getDate("ol_delivery_d"));
		}
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
			next_oid = (int) (row.getLong("d_next_oid"));
		}

		// query to get i_id
		int range = next_oid - l;
		BoundStatement bound_o_sel = new BoundStatement(stock_o_sel);
		results = session.execute(bound_o_sel.bind(w_id, d_id, range));

		// query to get s_qty
		String query_s_sel = "SELECT s_qty FROM " + keyspace
				+ ".stocks WHERE w_id= ? and i_id IN (";
		int i = 0;
		for (Row row : results) {
			if (i != 0) query_s_sel = query_s_sel + ",";
			i++;
			query_s_sel = query_s_sel + row.getInt("i_id");
		}
		query_s_sel = query_s_sel + ");";

		stock_s_sel = session.prepare(query_s_sel);
		BoundStatement bound_s_sel = new BoundStatement(stock_s_sel);
		results = session.execute(bound_s_sel.bind(w_id));

		int count = 0;
		for (Row row : results) {
			if (((int) (row.getLong("s_qty")/100)) < t)
				count++;
		}

		System.out.println("Total number of items where its stock quantity is below the threshold: " + count);

	}

	// Print popular item transaction details
	void popularItem(int w_id, int d_id, int l) {
		System.out.println("Popular Item transaction!");
		System.out.println("District identifiers !! w_id:" + w_id + ", d_id:" + d_id);
		System.out.println("Number of last orders to be examined:" + l);

		// query to next_oid
		int next_oid = 0;
		BoundStatement bound_d_sel = new BoundStatement(stock_d_sel);
		ResultSet results = session.execute(bound_d_sel.bind(w_id, d_id));
		for (Row row : results) {
			next_oid = (int) (row.getLong("d_next_oid"));
		}

		// query to get oid for last l orders
		int range = next_oid - l;
		BoundStatement bound_o_sel = new BoundStatement(popular_o_sel);
		results = session.execute(bound_o_sel.bind(w_id, d_id, range));

		// storing all values corresponding to oids
		TreeSet<Integer> OIDset = new TreeSet<Integer>();
		List <String> resList = new ArrayList<String>();
		for (Row row : results) {
			String temp = row.getInt("o_id") + "," + row.getString("c_name") + "," +  row.getDate("o_entry_d") + "," + row.getInt("ol_id") + "," + row.getInt("i_id")
					+ "," + row.getInt("ol_qty") + "," + row.getString("i_name") ;
			resList.add(temp);
			OIDset.add(row.getInt("o_id"));
		}

		// storing all popular item ids for all oids
		// o_id, c_name, o_entry_d, ol_id, i_id, ol_qty, i_name
		Map <Integer, String> popID = new HashMap<Integer, String>();
		Iterator <Integer> itr = OIDset.iterator();
		
		while(itr.hasNext()) {	
			int curOid = itr.next();
			
			//storing the max quantity ordered for each oid
			int tempMax=0;
			for(int j = 0; j< resList.size(); j++) {
				
				String[] vals = (resList.get(j)).split(",");
				
				if(curOid == Integer.parseInt(vals[0])) {
					if(tempMax < Integer.parseInt(vals[5])) {
						tempMax = Integer.parseInt(vals[5]);
						popID.put(Integer.parseInt(vals[4]),vals[6]);
					}
				}
			}
			// printing the popular item
			int i =0;
			for(int j = 0; j< resList.size(); j++) {
				
				String[] vals = (resList.get(j)).split(",");
				
				if((curOid == Integer.parseInt(vals[0])) && (tempMax == Integer.parseInt(vals[5]))) {
					if(i==0) System.out.println("Order number: " + curOid + ", entry date and time: " + vals[2] + ", Customer name: " + vals[1]);
					System.out.println("Item name:" + vals[6] + ", Quantity ordered: " + tempMax);
					i++;
				}
			}
			
		}
		
		// printing orders that contain the popular item
		for(Map.Entry<Integer, String> entry : popID.entrySet()) {
			int iid = entry.getKey();
			String iname = entry.getValue();
			System.out.println("Item Name: " + iname);
			
		}
		
	
	}
}
