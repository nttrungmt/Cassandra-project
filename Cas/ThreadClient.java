import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.HostDistance;

public class ThreadClient {
	private static Cluster cluster;
	private static Session session;
	private static String keyspace;
	public static int totalTransactions=0;
	public static double sumTime=0.0;
	public static int Threadcount=0;
	public static double startTime = 0.0;
	static int clientCount=0;

	// function to connect to the cluster
	public static void connect(String node) {
		PoolingOptions options = new PoolingOptions();
		options.setMaxConnectionsPerHost(HostDistance.LOCAL, 110)
		       .setCoreConnectionsPerHost(HostDistance.LOCAL, 100);
		Builder builder = Cluster.builder();
		builder.withReconnectionPolicy(new ConstantReconnectionPolicy(1000L));
		cluster = builder.addContactPoint(node).build();
		cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(600000000);
		cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(50000000);
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		// Get a session from your cluster and store the reference to it.
		session = cluster.connect();
	}

	//Close cluster
	public static void close() {
		cluster.close();
	}

	
	public static void main(String[] args) {

		// if the number of arguments are less than 3, return error
		if (args.length != 2) {
			System.err.println("Please enter two parameters: X and Z!");
			System.err.println("X: Database (D8 or D40) and Z = No. of clients (1 to 100)!");
			System.exit(-1);
		}

		// Validating the entered database
		if (!(args[0]).equals("D8") && !(args[0]).equals("D40")) {
			System.err.println("Incorrect value of database. Please choose D8 or D40");
			System.exit(-1);
		}
		else
		{
			keyspace = (args[0]).toLowerCase() +"key";
		}


		// Validating the entered client number
		clientCount = Integer.parseInt(args[1]);
		if (clientCount < 1 || clientCount > 100) {
			System.err
					.println("Incorrect value of Clients. Please choose a value between 1 to 100.");
			System.exit(-1);
		}

		connect("127.0.0.1");
		startTime = System.currentTimeMillis();
		for (int j = 0; j<(clientCount); j++)
		{
			String name = "Thread-"+j;
			TransThread Tj = new TransThread(name, j, session, keyspace);
			Tj.start();
		}

	}
}
