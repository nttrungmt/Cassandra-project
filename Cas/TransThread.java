import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class TransThread extends Thread {
	private Thread t;
	private String threadName;
	private int threadNum;
	private Session session;
	private String keyspace;
	private double startTime;
	private double endTime;

	TransThread(String name, int num, Session session, String keyspace) {
		threadName = name;
		threadNum = num;
		this.session = session;
		this.keyspace = keyspace;
		this.startTime = System.currentTimeMillis();
	}

	// Function to read the file and call transactions
	private int readFile() {
		int count = 0;
		BufferedReader br = null;

		try {
			String filename = String.valueOf(threadNum) + ".txt";
			Transaction transaction = new Transaction(session, keyspace);
			br = new BufferedReader(new FileReader(filename));

			String sCurrentLine;
			List<String> myList = new ArrayList<String>();

			// Copied data from file to list
			while ((sCurrentLine = br.readLine()) != null) {
				myList.add(sCurrentLine);
			}

			for (int i = 0; i < myList.size(); i++) {
				String[] words = (myList.get(i)).split(",");

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
					int[] item_num = new int[m];
					int[] supplier_warehouse = new int[m];
					long[] quantity = new long[m];
					for (int j = 0; j < (m); j++) {
						String[] vals = (myList.get(j+i+1)).split(",");
						item_num[j] = Integer.parseInt(vals[0]);
						supplier_warehouse[j] = Integer.parseInt(vals[1]);
						quantity[j] = (long)(Integer.parseInt(vals[2]));
					}
					i = i + m;
					transaction.newOrder(w_id, d_id, c_id, m, item_num, supplier_warehouse, quantity);
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
					transaction.orderStatus(Integer.parseInt(words[1]),
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
					transaction.popularItem(Integer.parseInt(words[1]),
							Integer.parseInt(words[2]),
							Integer.parseInt(words[3]));
					count++;
				} else {
					System.out.println("Invalid transaction");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InvalidQueryException e) {
			System.out.println("Invalid Query Exception Raised!");
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("Exception Raised!");
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				System.out.println("Exception Raised!");
				e.printStackTrace();
			}
		}
		return count;
	}

	public void run() {
		int count = 0;
		try {
			count = this.readFile();

		} catch (Exception e) {
			System.out.println("Thread-" + threadNum + " interrupted.");
		} finally {
			this.endTime = System.currentTimeMillis();
			double totalTime = (endTime - startTime)/1000;
			double throughput = count / totalTime;
			double avgTime = totalTime / count;
			System.err.println("Thread-" + threadNum + "-> transactions:" + count + ", time: " + totalTime + "sec, throughput: "+ throughput + ", Average Time: " + avgTime);
			
			ThreadClient.totalTransactions += count;
			ThreadClient.sumTime += totalTime;

			ThreadClient.Threadcount++;
			
			if (ThreadClient.Threadcount == ThreadClient.clientCount) {
				//sum of times taken by all transactions
				ThreadClient.sumTime = (ThreadClient.sumTime)/1000;
				//total time = end time - start time
				double total = ((System.currentTimeMillis()) - ThreadClient.startTime)/1000;
				
				double totalThroughput = ThreadClient.totalTransactions / total;
				double totalAvgTime = ThreadClient.sumTime / ThreadClient.totalTransactions;
				double avgTimeClient = ThreadClient.sumTime / ThreadClient.clientCount;
				double responseTime = total / ThreadClient.clientCount;
				
				System.out.println("Overall total number of transactions processed : "+ ThreadClient.totalTransactions);
				System.out.println("Overall Total time for processing the transactions (in sec): " + total);
				System.out.println("Transaction throughput (number of transactions processed per sec): "+ totalThroughput);
				System.out.println("Average time per transactions (in sec): : "+ totalAvgTime);
				System.out.println("Average time per client (in sec): : "+ avgTimeClient);
				System.out.println("Response time per client (in sec): : "+ responseTime);
			
				System.err.println("Overall total number of transactions processed : "+ ThreadClient.totalTransactions);
                                System.err.println("Overall Total time for processing the transactions (in sec): " + total);
                                System.err.println("Transaction throughput (number of transactions processed per sec): "+ totalThroughput);
                                System.err.println("Average time per transactions (in sec): : "+ totalAvgTime);
                                System.err.println("Average time per client (in sec): : "+ avgTimeClient);
                                System.err.println("Response time per client (in sec): : "+ responseTime);
	
				ThreadClient.close();
				System.exit(0);
			}
		}
	}

	public void start() {
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

}
