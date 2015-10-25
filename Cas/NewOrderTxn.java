//package test;

import java.sql.Time;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class NewOrderTxn {
	public static void NewOrder() {
		Cluster cluster;
        Session session;
        
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("testks");
        
		int W_ID = 1;
        int D_ID = 1;
        int C_ID = 101;
        int NUM_ITEMS = 1;
        int ITEM_NUMBER[] = {1};
		int SUPPLIER_WAREHOUSE[] = {1};
		int QUANTITY[] = {4};
		//String query;
        
        //get tax details
        int wtax = 0, dtax = 0, cdiscount = 0;
        //query = "select etax, dtax, cdiscount from cwd where wid = " + W_ID + " and cid = " + C_ID + " and did = " + D_ID;
        
        PreparedStatement statement = session.prepare("select wtax, dtax, cdiscount from cwd "
        		+ "where wid = ? and cid = ? and did = ?");
		BoundStatement cwdstmt = new BoundStatement(statement);
		long lasttime = System.nanoTime();
		ResultSet result =  session.execute(cwdstmt.bind(W_ID, C_ID, D_ID));
		long aftertime = System.nanoTime();
		long readduration = aftertime - lasttime;
		for (Row row : result) {
			wtax = row.getInt(0);
			dtax = row.getInt(1);
			cdiscount = row.getInt(2);
            System.out.println("From CWD Table: "
            		+ "\n\twtax = " + wtax
            		+ "\n\tdtax = " + dtax
            		+ "\n\tcdiscount = " + cdiscount);
        }
        
		//get stock details
		long sqty = 0;
		statement = session.prepare("select sqty from stocks where wid = ? and iid = ?");
		BoundStatement stocksstmt = new BoundStatement(statement);
		lasttime = System.nanoTime();
		result =  session.execute(stocksstmt.bind(W_ID, ITEM_NUMBER[0]));
		aftertime = System.nanoTime();
		readduration += (aftertime - lasttime);
		for (Row row : result) {
			sqty = row.getLong(0);
            System.out.println("From STOCKS Table: "
            		+ "\n\tsqty = " + sqty);
        }
		
        //get order details
        double iprice = 0.0;
		long N = 0;
		String iname = null;
		statement = session.prepare("select iname, iprice from ooli where iid = ? limit 1");
		BoundStatement itemstmt = new BoundStatement(statement);
		lasttime = System.nanoTime();
		result = session.execute(itemstmt.bind(ITEM_NUMBER[0]));
		aftertime = System.nanoTime();
		readduration += (aftertime - lasttime);
		for (Row row : result) {
			iname = row.getString(0);
			iprice = row.getDouble(1);
            System.out.println("From OOLI Table: "
            		+ "\n\tiname = " + iname
            		+ "\n\tiprice = " + iprice);
        }
        
        //increment Next order ID
        statement = session.prepare("select dnextoid from nextoids where wid = ? and did = ?");
//        		+ "set D_NEXT_O_ID = (D_NEXT_O_ID + 1)"
//        		+ " where D_W_ID = '" + W_ID + "' and D_ID = '" + D_ID + "'");
		BoundStatement nextoidstmt = new BoundStatement(statement);
		lasttime = System.nanoTime();
		result = session.execute(nextoidstmt.bind(W_ID, D_ID));
		aftertime = System.nanoTime();
		readduration += (aftertime - lasttime);
		for (Row row : result) {
			N = row.getLong(0);
            System.out.println("From NEXTOIDS Table: "
            		+ "\n\tdnextoid = " + N);
        }
		
		System.out.println("\nTime taken for Initial read: " + (readduration / 1000000) + "." + (readduration % 1000000) + " ms");
		
		//new order
        int O_W_ID = W_ID;
        int O_D_ID = D_ID;
        long O_ID = N;
        int O_C_ID = C_ID;
        int O_CARRIER_ID = 0;	//null
        double O_OL_CNT = NUM_ITEMS;
        int O_ALL_LOCAL = 0;	//TODO: need to calculate this later
        
//        statement = session.prepare("insert into Order (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)"
//        		+ " values ?,?,?,?,?,?,?,(now())");
//        session.execute(query);
        
        double TOTAL_AMOUNT = 0;
        // process all items
        double ADJUSTED_QTY = sqty - QUANTITY[0];
            
        ADJUSTED_QTY = ((ADJUSTED_QTY < 10) ? (ADJUSTED_QTY + 91) : ADJUSTED_QTY); 
        
        //update stock for item i and warehouse i
//        query = "update Stock set S_QUANTITY = " + ADJUSTED_QTY + ","
//        					  + " S_YTD = " + QUANTITY[i] + ","
//							  + " S_ORDER_CNT = (S_ORDER_CNT + 1),"
//							  + " S_REMOTE_CNT = (S_REMOTE_CNT + 1)"	// TODO: need to add condition here
//				+ " where S_W_ID = " + SUPPLIER_WAREHOUSE[i] + " and"
//					  + " S_I_ID = " + ITEM_NUMBER[i];
//        
        double ITEM_AMOUNT = QUANTITY[0] * iprice;
        TOTAL_AMOUNT += ITEM_AMOUNT;
        
        //new order line item
//        query = "insert into Order_Line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO)"
//        		+ " values (" + N + ", "			//OL_O_ID
//        					  + D_ID + ", "			//D_ID
//        					  + W_ID + ", "			//W_ID
//					  		  + i + ", "			//OL_NUMBER
//					  		  + ITEM_NUMBER[i] + ", "	//OL_I_ID
//					  		  + SUPPLIER_WAREHOUSE[i] + ", "	//OL_SUPPLY_W_ID
//					  		  + QUANTITY[i] + ", "	//OL_QUANTITY
//					  		  + ITEM_AMOUNT + ", "	//OL_AMOUNT
//					  		  + "0, "				//OL_DELIVERY_D
//					  		  + "S_DIST_xx)";		//OL_DIST_INFO, TODO: replace xx with D_ID
            
        TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + dtax + wtax) * (1 - cdiscount);
        
        statement = session.prepare("insert into ooli (wid, did, oid, olnum, cid, iid, iname, iprice, olamt)"
        		+ " values (?,?,?,?,?,?,?,?,?)");
        BoundStatement insooli = new BoundStatement(statement);
        lasttime = System.nanoTime();
        session.execute(insooli.bind(W_ID, D_ID, (int)N, NUM_ITEMS, C_ID, ITEM_NUMBER[0], iname, iprice, (int)ITEM_AMOUNT));
        aftertime = System.nanoTime();
        long upserttime = aftertime - lasttime;
        
        statement = session.prepare("update nextoids set dnextoid = dnextoid + 1 where wid = ? and did = ?");
        BoundStatement updnextoids = new BoundStatement(statement);
        lasttime = System.nanoTime();
        session.execute(updnextoids.bind(W_ID, D_ID));
        aftertime = System.nanoTime();
        upserttime += aftertime - lasttime;
        
        System.out.println("-----");
        statement = session.prepare("update stocks set sqty = sqty - ? where wid = ? and iid = ?");
        BoundStatement updstocks = new BoundStatement(statement);
        lasttime = System.nanoTime();
        session.execute(updstocks.bind((long)QUANTITY[0], W_ID, ITEM_NUMBER[0]));
        aftertime = System.nanoTime();
        upserttime += aftertime - lasttime;
        
        System.out.println("\nTime taken for Upserting: " + (upserttime / 1000000) + "." + (upserttime % 1000000) + " ms");
        
		cluster.close();
	}
}
