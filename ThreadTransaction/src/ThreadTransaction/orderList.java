package ThreadTransaction;

import java.sql.Timestamp;

public class orderList {
	
	private int ol_i_id;
	private int ol_supply_w_id;
	private int ol_quantity;
	private int ol_amount;
	private Timestamp ol_delivery_d;
	
	orderList(int ol_i_id, int ol_supply_w_id, int ol_quantity, int ol_amount, Timestamp ol_delivery_d)
	{
		this.ol_i_id = ol_i_id;
		this.ol_supply_w_id = ol_supply_w_id;
		this.ol_quantity = ol_quantity;
		this.ol_amount = ol_amount;
		this.ol_delivery_d = ol_delivery_d;
	}
	
	int get_ol_i_id()
	{
		return this.ol_i_id;
	}
	
	int get_ol_supply_w_id()
	{
		return this.ol_supply_w_id;
	}
	
	int get_ol_quantity()
	{
		return this.ol_quantity;
	}
	
	int get_ol_amount()
	{
		return this.ol_amount;
	}
	
	Timestamp get_ol_delivery_d()
	{
		return ol_delivery_d;
	}
}
