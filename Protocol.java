/*
 * Student number below:
 * 240524797
 */
/*Declaration of AI use:
 * 
 * I did use AI, ChatGPT, to aid me through this assignment - only to the extent I was permitted by the spec.
 * I DID NOT EVER:
 * - take any code from the AI
 * - Put any code into the AI
 * I put the assignment spec document into the AI, and used the AI to help structure how I tackled the assignment. 
 * By providing the AI with this document it was very aware that it could not output any code at all - as it states
 * this in the document. I also gave it tailored instructions for within that project to never output any code.
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
import java.net.SocketTimeoutException;
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


	public void sendMetadata() {

		BufferedReader br = null;
		try {
			 // validate inputs
			if (inputFile == null || !inputFile.exists()) {
				System.out.println("CLIENT: File does not exists");
				System.out.println("CLIENT: Exit ..");
				if (socket != null && !socket.isClosed()) socket.close();
				System.exit(0);
			}
	
			// Patch size must be >0
			if (maxPatchSize <= 0) {
				System.out.println("CLIENT: Invalid patch size (must be > 0). Transfer aborted.");
				if (socket != null && !socket.isClosed()) socket.close();
				System.out.println("CLIENT: Exit ..");
				System.exit(0);
			}
	
			// check output file name
			if (outputFileName == null || outputFileName.trim().isEmpty()) {
				System.out.println("CLIENT: Invalid output file name. Transfer aborted.");
				if (socket != null && !socket.isClosed()) socket.close();
				System.out.println("CLIENT: Exit ..");
				System.exit(0);
			}
	
			// count number of readings
			br = new BufferedReader(new FileReader(inputFile));
			fileTotalReadings = 0;
			String line;
			while ((line = br.readLine()) != null) {
				//ignore empty lines to avoid mis-counting
				if (!line.trim().isEmpty()) {
					fileTotalReadings++;
				}
			}
			br.close();
	
			// if no readings found, abort transfer
			if (fileTotalReadings == 0) {
				System.out.println("CLIENT: CSV file is empty. No readings to send. Transfer aborted.");
				if (socket != null && !socket.isClosed()) socket.close();
				System.out.println("CLIENT: Exit ..");
				System.exit(0);
			}
	
			 //assemble payload and make segment
			String payload = fileTotalReadings + "," + outputFileName + "," + maxPatchSize;
	
			Segment metaSeg = new Segment();
			metaSeg.setType(SegmentType.Meta);
			metaSeg.setSeqNum(0);
			metaSeg.setPayLoad(payload);
	
			 //print status and send
			System.out.println("CLIENT: META [SEQ#0] (Number of readings:" + fileTotalReadings
					+ ", patch size:" + maxPatchSize + ", file name: " + outputFileName + ")");
	
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(metaSeg);
			oos.flush();
			byte[] buf = baos.toByteArray();
	
			DatagramPacket packet = new DatagramPacket(buf, buf.length, ipAddress, portNumber);
			socket.send(packet);
	
		} catch (FileNotFoundException e) {
			System.err.println("CLIENT ERROR: CSV file not found: " + inputFile.getName());
			try { if (br != null) br.close(); } catch (IOException ignore) {}
			System.exit(0);
	
		} catch (IOException e) {
			System.err.println("CLIENT ERROR: Failed to send metadata. " + e.getMessage());
			try { if (br != null) br.close(); } catch (IOException ignore) {}
			System.exit(0);
		}
	}


	/* 
	 * This method read and send the next data segment (dataSeg) to the server. 
	 * See coursework specification for full details.
	 */
	public void readAndSend() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
	
		//skip readings already sent
		for (int i = 0; i < sentReadings; i++) {
			reader.readLine();
		}
	
		// prepare payload builder for up to maxPatchSize readings
		StringBuilder payloadBuilder = new StringBuilder();
		int count = 0;
		String line;
	
		while (count < maxPatchSize && (line = reader.readLine()) != null) {
			String[] parts = line.split(",");
			if (parts.length < 5) continue;
	
			String sensorId = parts[0];
			long timestamp = Long.parseLong(parts[1]);
			float[] values = new float[3];
			values[0] = Float.parseFloat(parts[2]);
			values[1] = Float.parseFloat(parts[3]);
			values[2] = Float.parseFloat(parts[4]);
	
			// create Reading object
			Reading reading = new Reading(sensorId, timestamp, values);
	
			if (count > 0) payloadBuilder.append(";");
			payloadBuilder.append(reading.toString());
			count++;
		}
		reader.close();
	
		// if no more readings left, finish cleanly
		if (count == 0) {
			System.out.println("CLIENT: No more readings to send. Transfer complete.");
			System.out.println("Total segments: " + totalSegments);
			System.exit(0);
		}
	
		// determine sseqNum dynamically
		int nextSeq = 1; // first DATA segment always starts with 1 after META
		if (ackSeg != null) {
			nextSeq = (ackSeg.getSeqNum() == 1) ? 0 : 1; // toggle based on last ACK
		}
	
		// build the seg
		dataSeg = new Segment(nextSeq, SegmentType.Data, payloadBuilder.toString(), payloadBuilder.length());
	
		// send the seg
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(byteStream);
		os.writeObject(dataSeg);
		os.flush();
	
		byte[] sendData = byteStream.toByteArray();
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, ipAddress, portNumber);
		socket.send(sendPacket);
	
		os.close();
		byteStream.close();
	
		totalSegments++;       // track total sent segments
		sentReadings += count; // increment readings sent
	
		// print confirmation
		System.out.println("------------------------------------------------------------------");
		System.out.println("CLIENT: Send: DATA [SEQ#" + dataSeg.getSeqNum() + "]"
				+ "(size:" + dataSeg.getSize()
				+ ", crc:" + dataSeg.calculateChecksum()
				+ ", content:" + dataSeg.getPayLoad() + ")");
		System.out.println("------------------------------------------------------------------");
	}

	/* 
	 * This method receives the current Ack segment (ackSeg) from the server 
	 * See coursework specification for full details.
	 */
	public boolean receiveAck() throws IOException {
		try {
			// prepare to receive ACK
			byte[] buffer = new byte[1024];
			DatagramPacket receivePacket = new DatagramPacket(buffer, buffer.length);
			socket.receive(receivePacket);
	
			// reconstruct byte stream into Segment object
			ByteArrayInputStream byteStream = new ByteArrayInputStream(receivePacket.getData());
			ObjectInputStream is = new ObjectInputStream(byteStream);
			ackSeg = (Segment) is.readObject();
			is.close();
			byteStream.close();
	
			// check that it is ACK segment
			if (ackSeg.getType() != SegmentType.Ack) {
				System.out.println("CLIENT: Received unexpected segment type. Expected ACK.");
				return false;
			}
	
			// verify seqNum matches
			if (ackSeg.getSeqNum() != dataSeg.getSeqNum()) {
				System.out.println("CLIENT: Received ACK with wrong sequence number. Expected " 
									+ dataSeg.getSeqNum() + " but got " + ackSeg.getSeqNum());
				return false;
			}
	
			// print confirmation
			System.out.println("CLIENT: RECEIVE: ACK [SEQ#" + ackSeg.getSeqNum() + "]");
			System.out.println("***************************************************************************************************");
	
			//if final ACK, end transfer
			if (sentReadings >= fileTotalReadings) {
				System.out.println("Total segments: " + totalSegments);
				System.exit(0);
			}
	
			return true;
	
		} catch (ClassNotFoundException e) {
			System.out.println("CLIENT: Error deserializing ACK segment: " + e.getMessage());
			return false;
		} catch (SocketTimeoutException e) {
			throw e; // let outer method handle timeouts
		} catch (IOException e) {
			System.out.println("CLIENT: Error receiving ACK segment: " + e.getMessage());
			return false;
		}
	}
	

	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission()  throws IOException {  
		// config the socket timeout
    socket.setSoTimeout(timeout);

    while (true) {
        try {
            if (receiveAck()) {            
                currRetry = 0;
                socket.setSoTimeout(0); 
                return;
            } 
            else {
                System.out.println("CLIENT: Invalid ACK sequence. Waiting again...");
            }

        } catch (SocketTimeoutException e) {
            // timeout expired so retransmit the same Data segment
            currRetry++;
            if (currRetry > maxRetries) {
                System.out.println("CLIENT: ERROR - Maximum retries (" + maxRetries + ") reached. Exiting transfer.");
                System.exit(0);
            }

            System.out.println("CLIENT: TIMEOUT ALERT");
            System.out.println("CLIENT: Re-sending the same segment again, current retry " + currRetry);
			System.out.println("------------------------------------------------------------------");
			System.out.println("CLIENT: Send: DATA [SEQ#" + dataSeg.getSeqNum() + "]"
					+ "(size:" + dataSeg.getSize()
					+ ", crc:" + dataSeg.calculateChecksum()
					+ ", content:" + dataSeg.getPayLoad() + ")");
			System.out.println("------------------------------------------------------------------");


            // re-send the same Data segment
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(byteStream);
            os.writeObject(dataSeg);
            os.flush();
            byte[] resendData = byteStream.toByteArray();
            DatagramPacket resendPacket = new DatagramPacket(resendData, resendData.length, ipAddress, portNumber);
            socket.send(resendPacket);
            os.close();
            byteStream.close();

            totalSegments++; // count the retransmission as a new sent segment
        } 
    }
}



	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss)  {
		int expectedSeqNum = 1;
		int lastAckedSeqNum = 0;
		long totalBytesReceived = 0;
		long totalUsefulBytes = 0;
		java.util.List<String> receivedLines = new java.util.ArrayList<>();
	
		try {
			// exit after 2000 ms of no data
			serverSocket.setSoTimeout(2000);
	
			while (true) {
				try {
					// receive a seg
					byte[] buffer = new byte[MAX_Segment_SIZE];
					DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
					serverSocket.receive(pkt);
	
					java.io.ByteArrayInputStream bais =
							new java.io.ByteArrayInputStream(pkt.getData(), 0, pkt.getLength());
					java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
					Segment seg = (Segment) ois.readObject();
					ois.close();
					bais.close();
	
					if (seg.getType() != SegmentType.Data) continue;
	
					System.out.println("------------------------------------------------------------------");
					System.out.println("SERVER: Receive: DATA [SEQ#" + seg.getSeqNum() + "]"
							+ "(size:" + seg.getSize()
							+ ", crc:" + seg.getChecksum()
							+ ", content:" + seg.getPayLoad() + ")");
	
					long calc = seg.calculateChecksum();
					System.out.println("SERVER: Calculated checksum is " + calc
							+ (calc == seg.getChecksum() ? "  VALID" : "  INVALID"));
					if (calc != seg.getChecksum()) continue;
	
					totalBytesReceived += seg.getSize();
					InetAddress clientAddr = pkt.getAddress();
					int clientPort = pkt.getPort();
	
					if (seg.getSeqNum() == expectedSeqNum) {
						// split readings on ';'
						String[] readings = seg.getPayLoad().split(";");
						int numReadings = readings.length;
					
						// sdd seg header line
						receivedLines.add("Segment [" + seg.getSeqNum() + "] has " + numReadings + " Readings");
					
						// add each reading to own line
						for (String r : readings) {
							receivedLines.add(r.trim());
						}
					
						receivedLines.add("");
					
						totalUsefulBytes += seg.getSize();
					
	
						if (!isLost(loss)) {
							Server.sendAck(serverSocket, clientAddr, clientPort, seg.getSeqNum());
						} else {
							System.out.println("SERVER: ACK [SEQ#" + seg.getSeqNum() + "] lost intentionally");
							System.out.println("------------------------------------------------");
							System.out.println("------------------------------------------------");
						}
	
						lastAckedSeqNum = seg.getSeqNum();
						expectedSeqNum = 1 - expectedSeqNum;   // alternate 1,0
					} else {
						// DUPLICATE segment
						System.out.println("SERVER: Duplicate DATA [SEQ#" + seg.getSeqNum() + "] detected");
						System.out.println("SERVER: Re-sending ACK [SEQ#" + lastAckedSeqNum + "] (may be lost)");
	
						if (!isLost(loss)) {
							Server.sendAck(serverSocket, clientAddr, clientPort, lastAckedSeqNum);
						} else {
							System.out.println("SERVER: ACK [SEQ#" + lastAckedSeqNum + "] lost intentionally");
							System.out.println("------------------------------------------------");
							System.out.println("------------------------------------------------");
						}
					}
	
				} catch (java.net.SocketTimeoutException e) {
					// end of transfer
					if (!receivedLines.isEmpty()) {
						Server.writeReadingsToFile(receivedLines, outputFileName);
					}
	
					double efficiency = (totalBytesReceived == 0)
							? 100.0
							: (totalUsefulBytes * 100.0) / (double) totalBytesReceived;
	
					System.out.printf("SERVER: Network efficiency = %.2f%%%n", efficiency);
					return;
	
				} catch (ClassNotFoundException e) {
					System.out.println("SERVER: Deserialization error: " + e.getMessage());
				}
			}
	
		} catch (IOException e) {
			System.out.println("SERVER: I/O error: " + e.getMessage());
		}
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
