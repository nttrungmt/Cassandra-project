package ThreadTransaction;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class ThreadClient {
	private Cluster cluster;
	private static Session session;
	private static String keyspace;
	private static int nodeCount;

	// function to connect to the cluster
	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		// Get a session from your cluster and store the reference to it.
		session = cluster.connect();
	}

	//Close cluster
	public void close() {
		cluster.close();
	}

	
	public static void main(String[] args) {

		// if the number of arguments are less than 3, return error
		if (args.length != 3) {
			System.out.println("Please enter three parameters: X, Y and Z!");
			System.out
					.println("X: Database (D8 or D40); Y: Nodes(1 or 2) and Z = No. of clients (1 to 100)!");
			System.exit(-1);
		}

		// Validating the entered database
		if (!(args[0]).equals("D8") && !(args[0]).equals("D40")) {
			System.out.println("Value received:" + args[0]
					+ ".Incorrect value of database. Please choose D8 or D40");
			System.exit(-1);
		}
		else
		{
			keyspace = args[0];
		}

		// Validating the entered Nodes
		if (!(args[1]).equals("1") && !(args[1]).equals("2")) {
			System.out.println("Value received:" + args[1]
					+ ".Incorrect value of Nodes. Please choose 1 or 2");
			System.exit(-1);
		}
		else
		{
			nodeCount = Integer.parseInt(args[1]);
		}

		// Validating the entered client number
		int clientCount = Integer.parseInt(args[2]);
		if (clientCount < 1 || clientCount > 100) {
			System.out
					.println("Value received:"
							+ args[2]
							+ ".Incorrect value of Clients. Please choose a value between 1 to 100.");
			System.exit(-1);
		}

		ThreadClient client = new ThreadClient();
		client.connect("192.168.8.128");
		for (int j = 0; j<clientCount; j++)
		{
			String name = "Thread-"+j;
			TransThread Tj = new TransThread(name, j, session, keyspace, nodeCount);
			Tj.start();
		}

		//client.close();
	}
}
