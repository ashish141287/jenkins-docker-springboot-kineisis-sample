package com.kinesis.kinesisconnectivity1;

//import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

@PropertySource(ignoreResourceNotFound = true, value = "classpath:application.properties")
@Component
public class KinesisConnection {

	public AmazonKinesis kinesis;

	@Value("${kinesis.stream.name}")	
	private String myStreamName;

	@Value("${aws.access.key}")	
	private String accessKey;

	@Value("${aws.secret.key}")	
	private String secretAccessKey;

	public void createKinesisConnection() {

		kinesis = AmazonKinesisClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(new AWSCredentials() {
					@Override
					public String getAWSAccessKeyId() {
						return accessKey;
					}

					@Override
					public String getAWSSecretKey() {
						return secretAccessKey;
					}
				}))
				.withRegion(Regions.AP_SOUTH_1).build();
		System.out.println(kinesis.describeStream(myStreamName));
	}

	/*
	 * public void pushDataStreams() {
	 * 
	 * kinesis = AmazonKinesisClientBuilder.standard() .withCredentials(new
	 * AWSStaticCredentialsProvider(new AWSCredentials() {
	 * 
	 * @Override public String getAWSAccessKeyId() { return accessKey; }
	 * 
	 * @Override public String getAWSSecretKey() { return secretAccessKey; } }))
	 * .withRegion(Regions.AP_SOUTH_1).build(); long createTime =
	 * System.currentTimeMillis(); PutRecordRequest putRecordRequest = new
	 * PutRecordRequest(); putRecordRequest.setStreamName(myStreamName); //
	 * System.out.println("data sent:\t" + array.toString()); JSONParser parser =
	 * new JSONParser(); try { Object obj = parser.parse(new
	 * FileReader("sampleData.json"));
	 * 
	 * // A JSON object. Key value pairs are unordered. JSONObject supports
	 * java.util.Map interface. JSONObject jsonObject = (JSONObject) obj; JSONArray
	 * array = new JSONArray(); array.add(jsonObject);
	 * putRecordRequest.setData(ByteBuffer.wrap(array.toString().getBytes())); //
	 * putRecordRequest.setData(ByteBuffer.wrap(String.format("test-data- // %d",
	 * createTime).getBytes()));
	 * putRecordRequest.setPartitionKey(String.format("partitionKey-%d",
	 * createTime)); PutRecordResult putRecordResult =
	 * kinesis.putRecord(putRecordRequest); System.out.
	 * printf("Successfully put record, partitionKey: %s, ShardId: %s, SequenceNumber : %s.\n"
	 * , putRecordRequest.getPartitionKey(), putRecordResult.getShardId(),
	 * putRecordResult.getSequenceNumber());
	 * 
	 * }catch(Exception e) {
	 * 
	 * e.printStackTrace();; } }
	 */

	public void getStreamData() {
		kinesis = AmazonKinesisClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(new AWSCredentials() {
					@Override
					public String getAWSAccessKeyId() {
						return accessKey;
					}

					@Override
					public String getAWSSecretKey() {
						return secretAccessKey;
					}
				}))
				.withRegion(Regions.AP_SOUTH_1).build();
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(myStreamName);
		DescribeStreamResult describeStreamResult;
		List<Shard> shards = new ArrayList<>();
		String lastShardId = null;
		do {
			describeStreamRequest.setExclusiveStartShardId(lastShardId);
			describeStreamResult = kinesis.describeStream(describeStreamRequest);
			shards.addAll(describeStreamResult.getStreamDescription().getShards());
			if (shards.size() > 0) {
				lastShardId = shards.get(shards.size() - 1).getShardId();
			}
		} while (describeStreamResult.getStreamDescription().getHasMoreShards());
		// Get Data from the Shards in a Stream
		// Hard-coded to use only 1 shard
		String shardIterator;
		GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
		getShardIteratorRequest.setStreamName(myStreamName);
		getShardIteratorRequest.setShardId(shards.get(0).getShardId());
		getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
		GetShardIteratorResult getShardIteratorResult = kinesis.getShardIterator(getShardIteratorRequest);
		shardIterator = getShardIteratorResult.getShardIterator();
		// Continuously read data records from shard.
		List<Record> records;
		while (true) {
			// Create new GetRecordsRequest with existing shardIterator.
			// Set maximum records to return to 1000.
			GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
			getRecordsRequest.setShardIterator(shardIterator);
			getRecordsRequest.setLimit(1000);
			GetRecordsResult result = kinesis.getRecords(getRecordsRequest);
			// Put result into record list. Result may be empty.
			records = result.getRecords();
			// Print records
			for (Record record : records) {
				ByteBuffer byteBuffer = record.getData();
				System.out.println(String.format("Seq No: %s - %s", record.getSequenceNumber(), new String(byteBuffer.array())));
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException exception) {
				throw new RuntimeException(exception);
			}
			shardIterator = result.getNextShardIterator();
		}
	}

}

