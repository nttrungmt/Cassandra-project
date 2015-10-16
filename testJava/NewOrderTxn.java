import jdk.nashorn.internal.runtime.regexp.joni.ast.QuantifierNode;

import com.datastax.driver.core.*;

public class NewOrderTxn {
	/*
	 * INPUT:
	 * (W_ID, D_ID, C_ID)
	 * NUM_ITEMS <= 15
	 * ITEM_NUMBER[i]
	 * SUPPIER_WAREHOUSE[i]
	 * QUANTITY[i]
	 */
	public void NewOrder(int W_ID, int D_ID, int C_ID, int NUM_ITEMS,
						 int ITEM_NUMBER[], int SUPPIER_WAREHOUSE[], int QUANTITY[]) {
		Cluster cluster;
        Session session;
        
        cluster = Cluster.builder().addContactPoint("192.168.8.128").build();
        session = cluster.connect("testkey");
        
        // simulate input
        int W_ID = 1;
        int D_ID = 1;
        int C_ID = 1;
        int NUM_ITEMS = 1;
        int ITEM_NUMBER[1] = {1001};
		int SUPPLIER_WAREHOUSE[1] = {1};
		int QUANTITY[1] = {4};
        
        String query;
        
        //get warehouse tax
        double W_TAX;
        query = "select W_TAX from Warehouse where W_ID = " + W_ID + "";
        ResultSet results = session.execute(query);
        for (Row row : results) {
            W_TAX = row.getDouble("W_TAX");
        }
        
        //get customer details
        double C_DISCOUNT;
        query = "select C_DISCOUNT from Customer where C_W_ID = " + W_ID + " and"
        											+ " C_D_ID = " + D_ID + " and"
        											+ " C_ID = " + C_ID;
        ResultSet results = session.execute(query);
        for (Row row : results) {
        	C_DISCOUNT = row.getDouble("C_DISCOUNT");
        }
        
        // get item details
        double I_PRICE[1];
        for (int i = 0; i < NUM_ITEMS; ++i) {
        	query = "select I_PRICE from Item where I_ID = " + ITEM_NUMBER[0] + "";
            results = session.execute(query);
            for (Row row : results) {
                I_PRICE[i] = row.getDouble("I_PRICE");
            }
        }
        
        //get stock details
        double S_QUANTITY[1];
        double S_YTD[1];
        double S_ORDER_CNT[1];
        double S REMOTE CNT[1];
        for (int i = 0; i < NUM_ITEMS; ++i) {
        	query = "select * from Stock"
        			+ " where S_W_ID = " + SUPPLIER_WAREHOUSE[i] + " and"
        				  + " S_I_ID = " + ITEM_NUMBER[i];
    					
        	results = session.execute(query);
            for (Row row : results) {
            	S_QUANTITY[i] = row.getDouble("S_QUANTITY");
            	S_YTD[i] = row.getDouble("S_YTD");
            	S_ORDER_CNT[i] = row.getDouble("S_ORDER_CNT");
            	S REMOTE CNT[i] = row.getDouble("S REMOTE CNT");
            }
        }
        
        //get next order number and district tax
        double D_TAX;
        int N;
        query = "select D_TAX, D_TAX, D_NEXT_O_ID from District"
        		+ " where D_W_ID = '" + W_ID + "' and D_ID = '" + D_ID + "'";
        results = session.execute(query);
        for (Row row : results) {
            D_TAX = row.getDouble("D_TAX");
        	N = row.getInt("D_NEXT_O_ID");
        } 
        
        //increment Next order ID
        query = "update District set D_NEXT_O_ID = (D_NEXT_O_ID + 1)"
        		+ " where D_W_ID = '" + W_ID + "' and D_ID = '" + D_ID + "'";
        session.execute(query);
        
        
        //new order
        int O_W_ID = W_ID;
        int O_D_ID = D_ID;
        int O_ID = N;
        int O_C_ID = C_ID;
        int O_CARRIER_ID = 0;	//null
        double O_OL_CNT = NUM_ITEMS;
        int O_ALL_LOCAL = 0;	//TODO: need to calculate this later
        
        query = "insert into Order (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)"
        		+ " values (" + O_W_ID + ", "		//O_W_ID
        					  +  O_D_ID + ", "		//O_D_ID
        					  + O_ID + ", "			//O_ID
        					  + O_C_ID + ", " 		//O_C_ID
        					  + O_CARRIER_ID + ", "	//O_CARRIER_ID
        					  + O_OL_CNT + ", "		//O_OL_CNT
        					  + O_ALL_LOCAL + ", " 	//O_ALL_LOCAL
        					  + "now())";			//O_ENTRY_D
        session.execute(query);
        
        double TOTAL_AMOUNT = 0;
        // process all items
        for (int i = 0; i < NUM_ITEMS; ++i) {
            double ADJUSTED_QTY = S_QUANTITY[i] - QUANTITY[i];
            
            ADJUSTED_QTY = ((ADJUSTED_QTY < 10) ? (ADJUSTED_QTY + 91) : ADJUSTED_QTY); 
            
            //update stock for item i and warehouse i
            query = "update Stock set S_QUANTITY = " + ADJUSTED_QTY + ","
            					  + " S_YTD = " + QUANTITY[i] + ","
    							  + " S_ORDER_CNT = (S_ORDER_CNT + 1),"
    							  + " S_REMOTE_CNT = (S_REMOTE_CNT + 1)"	// TODO: need to add condition here
					+ " where S_W_ID = " + SUPPLIER_WAREHOUSE[i] + " and"
						  + " S_I_ID = " + ITEM_NUMBER[i];
            
            double ITEM_AMOUNT = QUANTITY[i] * I_PRICE[i];
            TOTAL_AMOUNT += ITEM_AMOUNT;
            
            //new order line item
            query = "insert into Order_Line (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO)"
            		+ " values (" + N + ", "			//OL_O_ID
            					  + D_ID + ", "			//D_ID
            					  + W_ID + ", "			//W_ID
    					  		  + i + ", "			//OL_NUMBER
    					  		  + ITEM_NUMBER[i] + ", "	//OL_I_ID
    					  		  + SUPPLIER_WAREHOUSE[i] + ", "	//OL_SUPPLY_W_ID
    					  		  + QUANTITY[i] + ", "	//OL_QUANTITY
    					  		  + ITEM_AMOUNT + ", "	//OL_AMOUNT
    					  		  + "0, "				//OL_DELIVERY_D
    					  		  + "S_DIST_xx)";		//OL_DIST_INFO, TODO: replace xx with D_ID
            session.execute(query);
            
            TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + D_TAX + W_TAX) * (1 - C_DISCOUNT);
        }
        
        //TODO: show output info
	}
}