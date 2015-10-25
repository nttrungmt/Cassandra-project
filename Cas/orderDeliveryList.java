//package threadtrans;

import java.sql.Timestamp;

public class orderDeliveryList {
	
	private int ol_i_id;
	private String i_name;
	private int ol_supply_w_id;
	private double ol_quantity;
	private double ol_amount;
	private double s_quantity;
	private Timestamp ol_delivery_d;
	
	orderDeliveryList(int ol_i_id, String i_name, int ol_supply_w_id, double ol_quantity, double ol_amount, double s_quantity, Timestamp ol_delivery_d)
	{
		this.ol_i_id = ol_i_id;
		this.i_name = i_name;
		this.ol_supply_w_id = ol_supply_w_id;
		this.ol_quantity = ol_quantity;
		this.ol_amount = ol_amount;
		this.s_quantity = s_quantity;
		this.ol_delivery_d = ol_delivery_d;
	}
	
	int get_ol_i_id()
	{
		return this.ol_i_id;
	}
	
	String get_i_name()
	{
		return i_name;
	}
	
	int get_ol_supply_w_id()
	{
		return this.ol_supply_w_id;
	}
	
	double get_ol_quantity()
	{
		return this.ol_quantity;
	}
	
	double get_ol_amount()
	{
		return this.ol_amount;
	}
	
	Timestamp get_ol_delivery_d()
	{
		return ol_delivery_d;
	}
	
	double get_s_quantity()
	{
		return s_quantity;
	}
}
