import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;

import java.util.Base64;
import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

//import InFile;


public class Main {
    public static void main(String[] args) throws Exception {
//        Upload to S3
//        Read from S3
        FileReader fr = new FileReader("input/input-sample-1.txt");
        BufferedReader br = new BufferedReader(fr);



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
