import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Base64;
import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

//import InFile;


public class Main {
    public static void main(String[] args) throws Exception {
        // Get data from args
        File input = new File(args[0]);
        File output = new File(args[1]);
        int workerRatio = Integer.parseInt(args[2]);
        Boolean terminate = args.length == 4 && args[3].equals("terminate"); //TODO: Check with Moshe, What is 'terminate' type?

        // Get s3
        String bucket_name = "oo-dspsp-ass1";
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

        // Upload input to S3
        String key_name = generate_keyName();
        s3.putObject(PutObjectRequest.builder().bucket(bucket_name).key(key_name).build(), RequestBody.fromFile(input));

    }

    public static String generate_keyName(){
        String newKey = "inputTest";
        //TODO: Get available name
        return  newKey;
    }

    public static void awsTry(){
        Ec2Client ec2 = Ec2Client.create();
        String amiId = "ami-076515f20540e6e0b"; // Linux and Java 1.8
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T1_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(
                        "e".getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

    }
}
