package test;

public class testcassandra {
	public static void main(String[] args) {
		InsertIntoCounters.insertWarehouse();
		InsertIntoCounters.insertDistrict();
		InsertIntoCounters.insertStocks();
		InsertIntoCounters.insertCustomer();
	}
}
