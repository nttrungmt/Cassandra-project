import jdk.nashorn.internal.runtime.regexp.joni.ast.QuantifierNode;

import com.datastax.driver.core.*;

public class PaymentTxn {
	/*
	 * INPUT:
	 * (C_W_ID, C_D_ID, C_ID)
	 * PAYMENT
	 */
	public void Payment(int C_W_ID, int C_D_ID, int C_ID, double PAYMENT) {
		Cluster cluster;
        Session session;
        
        cluster = Cluster.builder().addContactPoint("192.168.8.128").build();
        session = cluster.connect("testkey");
        
        String query;
        
        //update warehouse
        query = "update Warehouse set W_YTD = (W_YTD + " + PAYMENT + ")"
        		+ " where W_ID = " + C_W_ID;
        session.execute(query);
        
        //update district
        query = "update District set D_YTD = (D_YTD + " + PAYMENT + ")"
        		+ " where D_ID = " + C_D_ID + " and"
        			  + " D_W_ID = " + C_W_ID;
        session.execute(query);
        
        //update customer 
        query = "update table Customer set C_BALANCE = (C_BALANCE - " + PAYMENT + "),"
        							   + " C_YTD_PAYMENT = (C_YTD_PAYMENT + " + PAYMENT + "),"
        							   + " C_PAYMENT_CNT = (C_PAYMENT_CNT + 1)"
        			+ " where C_W_ID = " + C_W_ID + " and"
        			  + " and C_D_ID = " + C_D_ID + " and"
        			  + " and C_ID = " + C_ID;
        session.execute(query);
        
        //TODO: show ouput info
	}
}