import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction {
	// variables
	private Session session;
	private String keyspace;
	private PreparedStatement neworder_d_sel;
    private PreparedStatement neworder_d_up;
    private PreparedStatement neworder_c_sel;
    private PreparedStatement neworder_s_sel;
    private PreparedStatement neworder_s_up;
    private PreparedStatement neworder_i_sel;
    private PreparedStatement neworder_o_in;
    private PreparedStatement neworder_del_in;
	private PreparedStatement payment_w_up;
	private PreparedStatement payment_d_up;
	private PreparedStatement payment_c_up;
	private PreparedStatement payment_cm_sel;
	private PreparedStatement payment_c_sel;
	private PreparedStatement del_d_sel;
	private PreparedStatement del_o_sel;
	private PreparedStatement del_o_up;
	private PreparedStatement del_d_up;
	private PreparedStatement del_c_up;
	private PreparedStatement orderst_o_sel;
	private PreparedStatement orderst_c_sel;
	private PreparedStatement stock_d_sel;
	private PreparedStatement stock_o_sel;
	private PreparedStatement stock_s_sel;
	private PreparedStatement popular_o_sel;

	// constructor
	Transaction(Session session, String keyspace) {
		this.session = session;
		this.keyspace = keyspace;

		//New Order Prepared Statements
        neworder_d_sel = session.prepare("SELECT d_next_oid FROM " + keyspace + ".district WHERE w_id = ? and d_id = ?");
        neworder_d_up = session.prepare("UPDATE " + keyspace + ".district SET d_next_oid = d_next_oid + 1 WHERE w_id = ? and d_id = ?");
        neworder_c_sel = session.prepare("SELECT c_name,d_tax,w_tax,c_discount,c_credit FROM " + keyspace + ".customermaster WHERE w_id = ? and d_id=? and c_id=?");
        neworder_s_sel = session.prepare("SELECT s_qty FROM " + keyspace + ".stocks WHERE w_id = ? and i_id = ?");
        neworder_s_up = session.prepare("UPDATE " + keyspace + ".stocks SET s_qty = s_qty -?, s_ytd = s_ytd + ?,s_order_cnt=s_order_cnt + 100,s_remote_cnt=s_remote_cnt+? WHERE w_id = ? and i_id = ?");
        neworder_i_sel = session.prepare("SELECT i_name,i_price FROM " + keyspace + ".itemstockmaster WHERE i_id = ? and w_id=?");
        neworder_o_in = session.prepare("INSERT into " + keyspace + ".orders( w_id , d_id , o_id , ol_id , c_id , i_id , i_name , i_price , ol_amount , ol_qty , o_carrier_id , ol_delivery_d , o_entry_d , ol_supply_w_id , c_name , o_ol_cnt , o_all_local , ol_dist_info ) values (?,?,?,?,?,?,?,?,?,?,?,'2000-01-01 00:00:00+0000',dateOf(now()),?,?,?,?,?)");
        neworder_del_in = session.prepare("INSERT into " + keyspace + ".delivery ( w_id, d_id , o_id , o_carrier_id) VALUES (?,?,?,0);");
        		
		// Payment Prepared Statements
		payment_w_up = session.prepare("UPDATE " + keyspace + ".warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?;");
		payment_d_up = session.prepare("UPDATE " + keyspace + ".district SET d_ytd = d_ytd + ? WHERE w_id = ? and d_id = ?;");
		payment_c_up = session.prepare("UPDATE " + keyspace + ".customer SET c_balance = c_balance + ?, c_ytd_payment = c_ytd_payment+ ?, c_payment_cnt = c_payment_cnt + 1 WHERE w_id = ? and d_id = ? and c_id = ?;");
		payment_cm_sel = session.prepare("SELECT c_name, c_addr, c_phone, c_since, c_credit, c_credit_lim, c_discount, w_addr, d_addr FROM "
						+ keyspace + ".customermaster WHERE w_id = ? and d_id = ? and c_id = ?;");
		payment_c_sel = session.prepare("SELECT c_balance FROM " + keyspace + ".customer WHERE w_id = ? and d_id = ? and c_id = ?;");

		// Delivery Prepared Statements
		del_d_sel = session.prepare("SELECT o_id FROM " + keyspace + ".delivery WHERE w_id = ? and d_id =? and o_carrier_id = 0 LIMIT 1;");
		del_o_sel = session.prepare("SELECT c_id,ol_id,ol_amount FROM " + keyspace + ".orders WHERE w_id = ? and d_id =? and o_id = ? and o_carrier_id = 0;");
		del_o_up = session.prepare("UPDATE " + keyspace + ".orders SET o_carrier_id = ?, ol_delivery_d = dateOf(now()) WHERE w_id =? and d_id = ? and o_id =? and ol_id = ?;");
		del_d_up = session.prepare("UPDATE " + keyspace + ".delivery SET o_carrier_id = ? WHERE w_id =? and d_id = ? and o_id =?;");
		del_c_up = session.prepare("UPDATE " + keyspace + ".customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt+100 WHERE w_id = ? and d_id = ? and c_id = ?;");

		// Order Status Prepared Statements
		orderst_o_sel = session.prepare("SELECT c_name, o_id, o_entry_d, o_carrier_id, i_id, ol_supply_w_id, ol_qty, ol_amount, ol_delivery_d FROM "
						+ keyspace + ".orders WHERE w_id = ? and d_id = ? and c_id = ? LIMIT 1;");
		orderst_c_sel = session.prepare("SELECT c_balance FROM " + keyspace + ".customer WHERE w_id = ? and d_id = ? and c_id = ?;");

		// stock level Prepared Statements
		stock_d_sel = session.prepare("SELECT d_next_oid FROM " + keyspace + ".district WHERE w_id = ? AND d_id = ? ;");
		stock_o_sel = session.prepare("SELECT i_id FROM " + keyspace + ".orders WHERE w_id = ? AND d_id = ? and o_id >=?;");

		// Popular Item Prepared Statements
		popular_o_sel = session.prepare("SELECT o_id, ol_id, i_id, ol_qty, o_entry_d, c_name, i_name, ol_qty FROM "
						+ keyspace + ".orders WHERE w_id = ? AND d_id = ? and o_id >=?;");
	}

	// New Order Transaction function
	void newOrder(int W_ID, int D_ID, int C_ID, int NUM_ITEMS, int[] ITEM_NUMBER, int[] SUPPLIER_WAREHOUSE,long[] QUANTITY) {
		long N = 0;
		int o_id = 0;
		int Qnty = 0;
		double w_tax = 0, d_tax = 0, c_discount = 0;
		String cname = null, c_credit = null;
		long updQty[] = new long[NUM_ITEMS];
		BoundStatement nextoidstmt = new BoundStatement(neworder_d_sel);
		ResultSet result = session.execute(nextoidstmt.bind(W_ID, D_ID));
		for (Row row : result) {
			N = row.getLong(0);
		}

		//update nextoid 
		BoundStatement updnextoid = new BoundStatement(neworder_d_up);
		session.execute(updnextoid.bind(W_ID, D_ID));

		//Inserting in delivery table
		BoundStatement stmt_del_in = new BoundStatement(neworder_del_in);
		session.execute(stmt_del_in.bind(W_ID, D_ID,((int)N)));
		
		BoundStatement custstmt = new BoundStatement(neworder_c_sel);
		result = session.execute(custstmt.bind(W_ID, D_ID, C_ID));
		for (Row row : result) {
			cname = row.getString(0);
			d_tax = row.getDouble(1);
			w_tax = row.getDouble(2);
			c_discount = row.getDouble(3);
			c_credit = row.getString(4);
		}
		String lastName = cname.substring(cname.lastIndexOf(" ") + 1);
		String output = "Customer's identifier :W_ID:" + W_ID + ", D_ID:" + D_ID + ", C_ID:" + C_ID + ", LastName:"
				+ lastName + ", Credit:" + c_credit + ", Discount:" + c_discount + "Warehouse tax rate:" + w_tax + ", District tax rate:" + d_tax + 
				"Order number:" + N + ", Entry Date:" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date());

		int O_OL_CNT = NUM_ITEMS;
		int O_ALL_LOCAL = 1;
		double TOTAL_AMOUNT = 0;
		long sqty[] = new long[NUM_ITEMS];
		double iprice[] = new double[NUM_ITEMS];
		double ADJUSTED_QTY[] = new double[NUM_ITEMS];
		long remoteCnt = 0;
		double ITEM_AMOUNT[] = new double[NUM_ITEMS];
		String iname[] = new String[NUM_ITEMS];

		for (int i = 0; i < NUM_ITEMS; i++) {
			if (SUPPLIER_WAREHOUSE[i] != W_ID) {
				O_ALL_LOCAL = 0;
				break;
			}
		}
		

		String OL_DIST_INFO = null;
		output = output + "Ordered items are:";
		for (int i = 0; i < NUM_ITEMS; i++) {

			BoundStatement stocksstmt = new BoundStatement(neworder_s_sel);
			result = session.execute(stocksstmt.bind(SUPPLIER_WAREHOUSE[i], ITEM_NUMBER[i]));
			for (Row row : result) {
				sqty[i] = row.getLong(0)/100;
			}

			ADJUSTED_QTY[i] = sqty[i] - QUANTITY[i];
			if (ADJUSTED_QTY[i] < 10) {
				updQty[i] = QUANTITY[i] - 91;
			} else {
				updQty[i] = QUANTITY[i];
			}
			int s_wid = SUPPLIER_WAREHOUSE[i];

			if (s_wid != W_ID) {
				remoteCnt = 100;

			}

			BoundStatement stocksstmt1 = new BoundStatement(neworder_s_up);
			result = session.execute(stocksstmt1.bind(updQty[i]*100, QUANTITY[i]*100, remoteCnt, s_wid, ITEM_NUMBER[i]));

			BoundStatement itemstmt = new BoundStatement(neworder_i_sel);
			result = session.execute(itemstmt.bind(ITEM_NUMBER[i], s_wid));
			for (Row row : result) {
				iname[i] = row.getString(0);
				iprice[i] = row.getDouble(1);
			}
			ITEM_AMOUNT[i] = QUANTITY[i] * iprice[i];
			TOTAL_AMOUNT += ITEM_AMOUNT[i];

			OL_DIST_INFO = "S_DIST_" + D_ID;
			o_id = (int) N;
			Qnty = (int) QUANTITY[i];

			BoundStatement ordstmt = new BoundStatement(neworder_o_in);
			//
			result = session.execute(ordstmt.bind(W_ID, D_ID, o_id, i + 1, C_ID, ITEM_NUMBER[i], iname[i], iprice[i],
					ITEM_AMOUNT[i], Qnty, 0, SUPPLIER_WAREHOUSE[i], cname, O_OL_CNT, O_ALL_LOCAL, OL_DIST_INFO));
			output = output + "ITEM_NUMBER:" + ITEM_NUMBER[i] + ", I_NAME:" + iname[i] + ", SUPPLIER_WAREHOUSE:"
					+ SUPPLIER_WAREHOUSE[i] + ", QUANTITY:" + Qnty + ", OL_AMOUNT:" + ITEM_AMOUNT[i] + ", S_QUANTITY:"
					+ sqty[i];
			
		}

		TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + d_tax + w_tax) * (1 - c_discount);
		System.out.println(output + "NUM_ITEMS:" + NUM_ITEMS + ", Total amount for order:" + TOTAL_AMOUNT);
	}

	// Payment Transaction function
	void payment(int w_id, int d_id, int c_id, double pay) {
		Long payment = new Long((long) (pay*100));
		String output = "Customer's identifier :w_id:" + w_id + ", d_id:" + d_id + ", c_id:" + c_id + ", Payment amount:" + pay;
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
			output = output + "Customer's Name : " + row.getString("c_name") + ", Address : " + row.getString("c_addr") + ", Credit" + row.getString("c_credit") + ", Credit_lim: " + row.getDouble("c_credit_lim") + ", Discount:" + row.getDouble("c_discount");
			for (Row row1 : results1) {
				output = output + ", Customer balance:" + ((double) (row1.getLong("c_balance")))/100;
			}
			output = output + "Warehouse’s address : " + row.getString("w_addr") + ", District’s address : " + row.getString("d_addr");
		}
		System.out.println(output);

	}

	// Delivery Transaction function
	void delivery(int w_id, int carrier_id) {

		for (int i = 1; i <= 10; i++) {
			BoundStatement bound_d_sel = new BoundStatement(del_d_sel);
			ResultSet results = session.execute(bound_d_sel.bind(w_id, i));
			int o_id = 0;
			for (Row row : results) {
				o_id = row.getInt("o_id");
			}
			//update delivery table for o_carrier_id
			BoundStatement bound_d_up = new BoundStatement(del_d_up);
			session.execute(bound_d_up.bind(carrier_id, w_id, i, o_id));
			
			// update order table for o_carrier_id
			BoundStatement bound_o_sel = new BoundStatement(del_o_sel);
			results = session.execute(bound_o_sel.bind(w_id, i, o_id));
			
			double B = 0.0;
			int c_id =0;
			for (Row row : results) {
				c_id = row.getInt("c_id");
				int ol_id = row.getInt("ol_id");
				B = B +  row.getDouble("ol_amount");
				
				BoundStatement bound_o_up = new BoundStatement(del_o_up);
				session.execute(bound_o_up.bind(carrier_id, w_id, i,
								o_id, ol_id));
			}
			B = B / 100;
			BoundStatement bound_c_up = new BoundStatement(del_c_up);
			session.execute(bound_c_up.bind((long)B, w_id, i, c_id));
		}

	}

	// order status transaction
	void orderStatus(int w_id, int d_id, int c_id) {

		BoundStatement bound_o_sel = new BoundStatement(orderst_o_sel);
		ResultSet results = session.execute(bound_o_sel.bind(w_id, d_id, c_id));

		BoundStatement bound_c_sel = new BoundStatement(orderst_c_sel);
		ResultSet results1 = session.execute(bound_c_sel.bind(w_id, d_id, c_id));
		String output = "";
		for (Row row : results) {
			output = output + "Customer's Name : " + row.getString("c_name");
			for (Row row1 : results1) {
				output = output + ", Balance: " + ((double) (row1.getLong("c_balance")))/100;
			}
			// for the last order
			output = output + "Customer’s last order : " + ", Order number : "+ row.getInt("o_id") + ", Entry date and time: " + row.getDate("o_entry_d") + ", Carrier identifier: " + row.getInt("o_carrier_id") + ", Item number: " + row.getInt("i_id") + ", Supplying warehouse number : " + row.getInt("ol_supply_w_id") + 
					", Quantity ordered: " + row.getInt("ol_qty") + ", Total price for ordered item: " + row.getDouble("ol_amount") + ", Data and time of delivery:" + row.getDate("ol_delivery_d");
		}
		System.out.println(output);
	}

	// Print Stock level transaction
	void stockLevel(int w_id, int d_id, int t, int l) {

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

		System.out.println("w_id:" + w_id + ", d_id:" + d_id + ", t:" + t
				+ ", l:" + l + "Total number of items where its stock quantity is below the threshold: " + count);

	}

	// Print popular item transaction details
	void popularItem(int w_id, int d_id, int l) {
		String output = "District identifiers !! w_id:" + w_id + ", d_id:" + d_id + ", Number of last orders to be examined:" + l;

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
					if(i==0) output = output + "Order number: " + curOid + ", entry date and time: " + vals[2] + ", Customer name: " + vals[1];
					output = output + "Item name:" + vals[6] + ", Quantity ordered: " + tempMax;
					i++;
				}
			}
			
		}
		
		// printing orders that contain the popular item
		for(Map.Entry<Integer, String> entry : popID.entrySet()) {
			int iid = entry.getKey();
			String iname = entry.getValue();
			output = output + "Item Name: " + iname;
			int count = 0;
			for(int j = 0; j< resList.size(); j++) {
				String[] vals = (resList.get(j)).split(",");
				// if the item is present
				if((iname.equals(vals[6])) && (iid == Integer.parseInt(vals[4]))) {
					count++;
				}
			}
			output = output + ", Number of orders in S containing the popular item:" + count;
		}
		System.out.println(output);
	}
	
}
