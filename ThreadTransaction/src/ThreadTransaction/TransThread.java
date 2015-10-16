package ThreadTransaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.Session;

public class TransThread extends Thread {
	private Thread t;
	private String threadName;
	private int threadNum;
	private Session session;
	private String keyspace;
	private int node;
	private long startTime;
	private long endTime;

	TransThread(String name, int num, Session session, String keyspace, int node) {
		threadName = name;
		threadNum = num;
		this.session = session;
		this.keyspace = keyspace;
		this.node = node;
		this.startTime = System.currentTimeMillis();
		System.out.println("Creating " + threadName);
	}

	// Function to read the file and call transactions
	private int readFile() {
		int count = 0;
		// File name
		String filename = String.valueOf(threadNum) + ".txt";

		BufferedReader br = null;
		Transaction transaction = new Transaction(session, keyspace, node);

		try {
			String sCurrentLine;

			br = new BufferedReader(new FileReader(filename));

			while ((sCurrentLine = br.readLine()) != null) {
				System.out.println("Reading file:" + filename + ", "
						+ sCurrentLine);
				String[] words = sCurrentLine.split(",");

				if ((words[0]).equals("N")) {
					/*
					 * New order transaction - C_ID, W_ID, D_ID, M and OL_I_ID,
					 * OL_SUPPLY_W_ID, OL_QUANTITY.
					 */
					int c_id = Integer.parseInt(words[1]);
					int w_id = Integer.parseInt(words[2]);
					int d_id = Integer.parseInt(words[3]);
					int m = Integer.parseInt(words[4]);

					// read the items in the new order
					int j = 0;

					String localLine;
					Set<orderList> orderSet = new HashSet<orderList>();
					while ((localLine = br.readLine()) != null) {
						String[] localValues = localLine.split(",");
						orderList orderlist = new orderList(
								Integer.parseInt(localValues[0]),
								Integer.parseInt(localValues[1]),
								Integer.parseInt(localValues[2]), 0, null);
						orderSet.add(orderlist);
						j++;
						if (j == m) {
							break;
						}
					}
					transaction.newOrder(w_id, d_id, c_id, m, orderSet);
					count++;
				} else if ((words[0]).equals("P")) {
					// Payment transaction -> C_W_ID, C_D_ID, C_ID, PAYMENT.
					int w_id = Integer.parseInt(words[1]);
					int d_id = Integer.parseInt(words[2]);
					int c_id = Integer.parseInt(words[3]);
					double payment = Double.parseDouble(words[4]);
					transaction.payment(w_id, d_id, c_id, payment);
					count++;
				} else if ((words[0]).equals("D")) {
					// Delivery transaction -> W_ID, CARRIER_ID
					transaction.delivery(Integer.parseInt(words[1]),
							Integer.parseInt(words[2]));
					count++;
				} else if ((words[0]).equals("O")) {
					// Order Status transaction -> C_W_ID, C_D_ID, C_ID
					transaction.printOrderStatus(Integer.parseInt(words[1]),
							Integer.parseInt(words[2]),
							Integer.parseInt(words[3]));
					count++;
				} else if ((words[0]).equals("S")) {
					// Stock level transaction -> W_ID, D_ID, T, L
					transaction.stockLevel(Integer.parseInt(words[1]),
							Integer.parseInt(words[2]),
							Integer.parseInt(words[3]),
							Integer.parseInt(words[4]));
					count++;
				} else if ((words[0]).equals("I")) {
					// Popular Item transaction -> W_ID, D_ID, L
					transaction.printPopularItem(Integer.parseInt(words[1]),
							Integer.parseInt(words[2]),
							Integer.parseInt(words[3]));
					count++;
				} else {
					System.out.println("Invalid transaction");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return count;
	}

	public void run() {
		System.out.println("Running " + threadName);
		int count = 1;
		try {
			count = this.readFile();

		} catch (Exception e) {
			System.out.println("Thread " + threadName + " interrupted.");
		}
		System.out.println("Thread " + threadName + " exiting.");
		System.err.println("Total number of transactions processed : " + count);
		this.endTime = System.currentTimeMillis();
		float totalTime = endTime - startTime;
		System.err
				.println("Total elapsed time for processing the transactions (in seconds): "
						+ totalTime);
		float throughput = count / totalTime;
		System.err
				.println("Transaction throughput (number of transactions processed per second): "
						+ throughput);
		ThreadClient.totalTransactions += count;
		ThreadClient.totalTime += totalTime;

		System.err.println("Overall total number of transactions processed : "
				+ ThreadClient.totalTransactions);
		System.err
				.println("Overall Total elapsed time for processing the transactions (in seconds): "
						+ ThreadClient.totalTime);
		System.err
				.println("Transaction throughput (number of transactions processed per second): "
						+ ((ThreadClient.totalTransactions) / (ThreadClient.totalTime)));

	}

	public void start() {
		System.out.println("Starting " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

}