/*
 * Replace the following string of 0s with your student number
 * 000000000
 */
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class Protocol {

	static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
	static final String	 TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
	static final String	 LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
	static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
	static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
	public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

	/*
	 * The following attributes control the execution of the transfer protocol and provide access to the 
	 * resources needed for the transfer 
	 * 
	 */ 

	private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
	private int portNumber; 		    // the  port the server is listening on
	private DatagramSocket socket;      // the socket that the client binds to

	private File inputFile;            // the client-side CSV file that has the readings to transfer  
	private String outputFileName ;    // the name of the output file to create on the server to store the readings
	private int maxPatchSize;		   // the patch size - no of readings to be sent in the payload of a single Data segment

	private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server 
	private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

	private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
	private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
	private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

	private int fileTotalReadings;    // number of all readings in the csv file
	private int sentReadings;         // number of readings successfully sent and acknowledged
	private int totalSegments;        // total segments that the client sent to the server
	private int seqNum = 1;


	// Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
	public static Protocol instance = new Protocol();

	/**************************************************************************************************************************************
	 **************************************************************************************************************************************
	 * For this assignment, you have to implement the following methods:
	 *		sendMetadata()
	 *      readandSend()
	 *      receiveAck()
	 *      startTimeoutWithRetransmission()
	 *		receiveWithAckLoss()
	 * Do not change any method signatures, and do not change any other methods or code provided.
	 ***************************************************************************************************************************************
	 **************************************************************************************************************************************/
	/* 
	 * This method sends protocol metadata to the server.
	 * See coursework specification for full details.	
	 */
	public void sendMetadata()   { 

    BufferedReader br = null;
    try {
        //count total number of readings (lines) in the csv
        br = new BufferedReader(new FileReader(inputFile));
        fileTotalReadings = 0;
        while (br.readLine() != null) {
            fileTotalReadings++;
        }
        br.close();

        //out togetehr metadata payload
        String payload = fileTotalReadings + "," + outputFileName + "," + maxPatchSize;

        // create Meta segment with seqNum = 0
        Segment metaSeg = new Segment();
        metaSeg.setType(SegmentType.Meta);
        metaSeg.setSeqNum(0);
        metaSeg.setPayLoad(payload);

        // print status
        System.out.println("CLIENT: META [SEQ#0] (Number of readings:" + fileTotalReadings
                + ", file name:" + outputFileName + ", patch size:" + maxPatchSize + ")");

        // sendto server

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(metaSeg);
		oos.flush();
		byte[] buf = baos.toByteArray();

		DatagramPacket packet = new DatagramPacket(buf, buf.length, ipAddress, portNumber);
		socket.send(packet);


    } catch (FileNotFoundException e) {
        System.err.println("CLIENT ERROR: CSV file not found: " + inputFile.getName());
        if (br != null) {
            try { br.close(); } catch (IOException ex) {}
        }
        System.exit(0);
    } catch (IOException e) {
        System.err.println("CLIENT ERROR: Failed to send metadata. " + e.getMessage());
        try {
            if (br != null) br.close();
        } catch (IOException ex) {}
        System.exit(0);
    }	
	} 


	/* 
	 * This method read and send the next data segment (dataSeg) to the server. 
	 * See coursework specification for full details.
	 */
	public void readAndSend() throws IOException { 
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));

		// skip csv lines already done
		for (int i = 0; i < sentReadings; i++) {
			reader.readLine();
		}

		// limit at maxpatchsize
		StringBuilder payloadBuilder = new StringBuilder();
		int count = 0;
		String line;

		//split line by commas and into various args
		while (count < maxPatchSize && (line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts.length < 5) continue;

			String sensorId = parts[0];
			long timestamp = Long.parseLong(parts[1]);
			float[] values = new float[3];
			values[0] = Float.parseFloat(parts[2]);
			values[1] = Float.parseFloat(parts[3]);
			values[2] = Float.parseFloat(parts[4]);

			//reading object
			Reading reading = new Reading(sensorId, timestamp, values);

			if (count > 0) payloadBuilder.append(";");
			payloadBuilder.append(reading.toString());
			count++;
		}

		reader.close();
		//if no readings found terminate 
		if (count == 0) {
			System.out.println("CLIENT: No more readings to send. Transfer complete.");
			System.out.println("Total segments: " + totalSegments);
			System.exit(0);
		}

		//build the data segment
		dataSeg = new Segment(seqNum, SegmentType.Data, payloadBuilder.toString(), payloadBuilder.length());


		// serialise and send
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(byteStream);
		os.writeObject(dataSeg);
		os.flush();
		byte[] sendData = byteStream.toByteArray();
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, ipAddress, portNumber);
		socket.send(sendPacket);
		os.close();
		byteStream.close();

		// update counters
		totalSegments++;
		sentReadings += count;

		// print progress
		System.out.println("CLIENT: Send: DATA [SEQ#" + dataSeg.getSeqNum() + "]"
				+ "(size:" + dataSeg.getSize()
				+ ", crc:" + dataSeg.getChecksum()
				+ ", content:" + dataSeg.getPayLoad() + ")");
	}
	

	/* 
	 * This method receives the current Ack segment (ackSeg) from the server 
	 * See coursework specification for full details.
	 */
	public boolean receiveAck() throws IOException { 
		try {
			// prepare to receive Ack
			byte[] buffer = new byte[1024];
			DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);

			socket.receive(receivePacket);

			// reconstrut byte stream into Segment object
			ByteArrayInputStream byteStream = new ByteArrayInputStream(receivePacket.getData());
			ObjectInputStream is = new ObjectInputStream(byteStream);
			ackSeg = (Segment) is.readObject();
			is.close();
			byteStream.close();

			// check that it is indeed an ACK segment
			if (ackSeg.getType() != SegmentType.Ack) {
				System.out.println("CLIENT: Received unexpected segment type. Expected ACK.");
				return false;
			}

			// verify seqNum
			if (ackSeg.getSeqNum() != seqNum) {
				System.out.println("CLIENT: Received ACK with wrong sequence number. Expected " + seqNum + " but got " + ackSeg.getSeqNum());
				return false;
			}

			// print confirmation 
			System.out.println("CLIENT: RECIEVE: ACK [SEQ#" + ackSeg.getSeqNum() + "]");
			System.out.println("***************************************************************************************************");

			// check if this was the final ACK 
			if (sentReadings >= fileTotalReadings) {
				System.out.println("Total segments: " + totalSegments);
				System.exit(0);
			}

			// alternate seq num
			seqNum = 1 - seqNum;

			return true;

		} catch (ClassNotFoundException e) {
			System.out.println("CLIENT: Error deserializing ACK segment: " + e.getMessage());
			return false;
		} catch (IOException e) {
			System.out.println("CLIENT: Error receiving ACK segment: " + e.getMessage());
			return false;
		}
	}

	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission()   {  
		System.exit(0);
	}


	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss)  {
		System.exit(0);
	}


	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}
