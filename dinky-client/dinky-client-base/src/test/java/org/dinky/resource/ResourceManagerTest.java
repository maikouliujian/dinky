package org.dinky.resource;

import org.dinky.data.enums.Status;
import org.dinky.data.model.SystemConfiguration;
import org.dinky.resource.impl.OssResourceManager;
import org.dinky.resource.impl.S3ResourceManager;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;

/**
 * @author liujian
 * @create 2024/9/21 15:31
 * @description
 */
public class ResourceManagerTest {
    @Test
    public void ossTest() {
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_MODEL.getKey(), "OSS");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_ACCESSKEY.getKey(), "ak");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_SECRETKEY.getKey(), "sk");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_BUCKETNAME.getKey(), "mybucket");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_REGION.getKey(), "ap-northeast-1");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_ENDPOINT.getKey(), "https://xxxx.amazonaws.com");
        //SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_ENDPOINT.getKey(), "http://localhost:9000");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_UPLOAD_BASE_PATH.getKey(), "/dinky");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_PATH_STYLE_ACCESS.getKey(), "false");
        BaseResourceManager.initResourceManager();
        OssResourceManager instance = (OssResourceManager) BaseResourceManager.getInstance();
        //List<Bucket> allBuckets = instance.getOssTemplate().getAllBuckets();
        //allBuckets.forEach(System.out::println);
        instance.putFile("dinkytest/111111111112.log", new File("/Users/admin/Downloads/1726790982402.log"));

    }

    @Test
    public void test1() {
        S3Client s3Client = S3Client.builder()
                .region(Region.of("ap-northeast-1"))
                .credentialsProvider(() -> AwsBasicCredentials.create("ak", "sk"))
                .build();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("mybucket")
                .key("dolphinscheduler/dolphin/resources/bin/check-partition-existence.sh")
                .build();
        ResponseInputStream<GetObjectResponse> s3is = s3Client.getObject(getObjectRequest);

//        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
//                .bucket("mybucket")
//                .key("dolphinscheduler/dolphin/resources/bin/check-partition-existence.sh")
//                .build();
//        ResponseInputStream<GetObjectResponse> s3is = s3Client.getObject(getObjectRequest);
//        try (FileOutputStream fos = new FileOutputStream("./aaaaaa")) {
//            byte[] readBuf = new byte[1024];
//            int readLen;
//            while ((readLen = s3is.read(readBuf)) > 0) {
//                fos.write(readBuf, 0, readLen);
//            }
//        } catch (AmazonServiceException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        try {
//            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket("mybucket")
//                    .key("dinkytest/1726790982402.log") //todo 注意key前面不能有/
//                    .build();
//            s3Client.putObject(putObjectRequest, RequestBody.fromFile(new File("/Users/admin/Downloads/1726790982402.log")));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
    @Test
    public void s3Test() {
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_MODEL.getKey(), "OSS");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_ACCESSKEY.getKey(), "ak");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_SECRETKEY.getKey(), "sk");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_BUCKETNAME.getKey(), "mybucket");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_REGION.getKey(), "ap-northeast-1");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_ENDPOINT.getKey(), "https://xxxx.amazonaws.com");
        //SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_OSS_ENDPOINT.getKey(), "http://localhost:9000");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_UPLOAD_BASE_PATH.getKey(), "/dinky");
        SystemConfiguration.getInstances().setConfiguration(Status.SYS_RESOURCE_SETTINGS_PATH_STYLE_ACCESS.getKey(), "false");
        BaseResourceManager.initResourceManager();
        S3ResourceManager instance = (S3ResourceManager) BaseResourceManager.getInstance();
        //List<Bucket> allBuckets = instance.getOssTemplate().getAllBuckets();
        //allBuckets.forEach(System.out::println);
        //instance.putFile("dinkytest/111111111113.log", new File("/Users/admin/Downloads/1726790982402.log"));
       String fileContent = instance.getFileContent("dolphinscheduler/dolphin/resources/bin/check-partition-existence.sh");
        System.out.println(fileContent);

    }
}
