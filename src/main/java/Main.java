import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;

import java.util.Base64;

public class Main {
    public static void main(String[] args) {
        System.out.println("HelloWorld");
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
