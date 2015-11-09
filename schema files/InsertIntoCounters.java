import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class InsertIntoCounters {
	static String path = "/temp/group10/D8data";
	static String node = "127.0.0.1";
	static String keyspace = "d8key";

	public void insertWarehouse() {
		String fileName = path + "/warehousecsv.csv";

		// This will reference one line at a time
		String line = null;
		Cluster cluster;
		Session session;
		String statement;

		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect(keyspace);

		BufferedReader bufferedReader = null;
		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader
			bufferedReader = new BufferedReader(fileReader);

			// int count = 0;
			while ((line = bufferedReader.readLine()) != null) {
				String[] words = line.split(",");

				StringTokenizer tok = new StringTokenizer(words[1], ",.");
				int wytd = 0;
				if (tok.hasMoreTokens()) {
					wytd = Integer.parseInt(tok.nextToken());
				}
				statement = "update warehouse set w_ytd = w_ytd + " + wytd + " where w_id = " + words[0];
				session.execute(statement);

				System.out.println("Warehouse -->> " + words[0] + ":" + words[1]);
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		} finally {
		}

		cluster.close();
		System.out.println("Warehouse Updated.");
	}

	public void insertDistrict() {
		String fileName = path + "/districtcsv.csv";

		// This will reference one line at a time
		String line = null;
		Cluster cluster;
		Session session;
		String statement;

		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect(keyspace);

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while ((line = bufferedReader.readLine()) != null) {
				String[] words = line.split(",");

				StringTokenizer tok = new StringTokenizer(words[2], ",.");
				int dnextoid = 0;
				if (tok.hasMoreTokens()) {
					dnextoid = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[3], ",.");
				int dytd = 0;
				if (tok.hasMoreTokens()) {
					dytd = Integer.parseInt(tok.nextToken());
				}

				statement = "update district set d_next_oid = d_next_oid + " + dnextoid + " , d_ytd= d_ytd + " + dytd
						+ " where w_id = " + words[0] + " and d_id=" + words[1];
				session.execute(statement);

				System.out.println("District -->> " + words[0] + ":" + words[1]);
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		}

		cluster.close();
		System.out.println("District Updated.");
	}

	public void insertStocks() {
		String fileName = path + "/stockscsv.csv";

		// This will reference one line at a time
		String line = null;
		Cluster cluster;
		Session session;
		String statement;

		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect(keyspace);

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				String[] words = line.split(",");

				StringTokenizer tok = new StringTokenizer(words[2], ",.");
				int sqty = 0;
				if (tok.hasMoreTokens()) {
					sqty = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[3], ",.");
				int sordercnt = 0;
				if (tok.hasMoreTokens()) {
					sordercnt = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[4], ",.");
				int sremotecnt = 0;
				if (tok.hasMoreTokens()) {
					sremotecnt = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[5], ",.");
				int sytd = 0;
				if (tok.hasMoreTokens()) {
					sytd = Integer.parseInt(tok.nextToken());
				}

				statement = "update stocks set s_qty = s_qty + " + sqty + " , s_order_cnt= s_order_cnt + " + sordercnt
						+ " , s_remote_cnt= s_remote_cnt + " + sremotecnt + " , s_ytd= s_ytd + " + sytd
						+ " where w_id = " + words[0] + " and i_id=" + words[1];
				session.execute(statement);

				System.out.println("Stocks -->> " + words[0] + ":" + words[1]);
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		}

		cluster.close();
		System.out.println("Stocks Updated.");
	}

	public void insertCustomer() {
		String fileName = path + "/customercsv.csv";

		// This will reference one line at a time
		String line = null;
		Cluster cluster;
		Session session;
		String statement;

		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect(keyspace);

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				String[] words = line.split(",");

				StringTokenizer tok = new StringTokenizer(words[3], ",.");
				int cytdpayment = 0;
				if (tok.hasMoreTokens()) {
					cytdpayment = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[4], ",.");
				int cbalance = 0;
				if (tok.hasMoreTokens()) {
					cbalance = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[5], ",.");
				int cpaymentcnt = 0;
				if (tok.hasMoreTokens()) {
					cpaymentcnt = Integer.parseInt(tok.nextToken());
				}

				tok = new StringTokenizer(words[6], ",.");
				int cdeliverycnt = 0;
				if (tok.hasMoreTokens()) {
					cdeliverycnt = Integer.parseInt(tok.nextToken());
				}

				statement = "update customer set c_ytd_payment = c_ytd_payment + " + cytdpayment
						+ ", c_balance  = c_balance + " + cbalance + " , c_payment_cnt = c_payment_cnt + " + cpaymentcnt
						+ " , c_delivery_cnt = c_delivery_cnt + " + cdeliverycnt + " where w_id = " + words[0]
						+ " and d_id = " + words[1] + " and c_id = " + words[2];
				session.execute(statement);

				System.out.println("Customer -->> " + words[0] + ":" + words[1] + ":" + words[2]);
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		}

		cluster.close();
		System.out.println("Customer Updated.");
	}

    public static void main(String[] args) {
        InsertIntoCounters is = new InsertIntoCounters();
        is.insertWarehouse();
        is.insertDistrict();
        is.insertStocks();
        is.insertCustomer();
    }
}
