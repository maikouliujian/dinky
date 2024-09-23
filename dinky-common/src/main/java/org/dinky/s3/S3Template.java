/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dinky.s3;

import com.amazonaws.AmazonServiceException;
import org.dinky.data.properties.OssProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.utils.StringUtils;

import java.io.*;
import java.net.URI;
import java.util.*;


public class S3Template {
    private static final Logger logger = LoggerFactory.getLogger(S3Template.class);
    private S3Client s3Client = null;
    private final OssProperties ossProperties;
    public S3Template(OssProperties ossProperties) {
        this.ossProperties = ossProperties;
        String accesskeyId = ossProperties.getAccessKey();
        String secretKeyId = ossProperties.getSecretKey();
        String endPoint = ossProperties.getEndpoint();
        String region = ossProperties.getRegion();
        String bucketName = ossProperties.getBucketName();
        StsClient stsClient = null;
        if(StringUtils.isNotBlank(accesskeyId) && StringUtils.isNotBlank(secretKeyId)) {
            if (StringUtils.isNotBlank(endPoint)) {
                s3Client = S3Client.builder()
                        .region(Region.of(region))
                        .credentialsProvider(() -> AwsBasicCredentials.create(accesskeyId, secretKeyId))
                        .endpointOverride(URI.create(endPoint))
                        .build();
                stsClient = StsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(() -> AwsBasicCredentials.create(accesskeyId, secretKeyId))
                        .endpointOverride(URI.create(endPoint))
                        .build();
            } else {
                s3Client = S3Client.builder()
                        .region(Region.of(region))
                        .credentialsProvider(() -> AwsBasicCredentials.create(accesskeyId, secretKeyId))
                        .build();
                stsClient = StsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(() -> AwsBasicCredentials.create(accesskeyId, secretKeyId))
                        .build();
            }
        } else {
            s3Client = S3Client.builder()
                    .region(Region.of(region))
                    .credentialsProvider(DefaultCredentialsProvider.builder().build())
                    .build();
            stsClient = StsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(DefaultCredentialsProvider.builder().build())
                    .build();
        }
        checkBucketNameExists(bucketName, region);
        try {
            GetCallerIdentityResponse response = stsClient.getCallerIdentity();
            String accountId = response.account();
            String userId = response.userId();
            String arn = response.arn();
            logger.info("stsClient account id:{},user id:{},arn:{}.", accountId, userId, arn);
        } catch (Exception e) {
            logger.error("stsClient getCallerIdentity error.", e);
        } finally {
            if (stsClient != null) stsClient.close();
        }
    }

    private void checkBucketNameExists(String bucketName, String region) {
        if (StringUtils.isBlank(bucketName)) {
            throw new IllegalArgumentException("resource.aws.s3.bucket.name is blank");
        }
        boolean isBucketExist = doesBucketExist(bucketName);
        if (!isBucketExist) {
            throw new IllegalArgumentException(
                    "bucketName: " + bucketName + " is not exists, you need to create them by yourself");
        }
        logger.info("bucketName: {} has been found, the current regionName is {}", bucketName, region);
    }

    private boolean doesBucketExist(String bucketName) {
        try {
            HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.headBucket(headBucketRequest);
            return true;
        } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
            logger.error("doesBucketExist fail", e);
            return false;
        } catch (Exception e){
            logger.error("doesBucketExist exception fail", e);
            return false;
        }
    }
    public ResponseInputStream<GetObjectResponse> getObject(String bucketName, String objectName) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectName)
                .build();
        return s3Client.getObject(getObjectRequest);
    }

    public void putObject(String bucketName, String objectName, InputStream stream) throws IOException {
        upload(bucketName, objectName, stream);
    }

    public boolean upload(String bucketName, String objectName, InputStream stream) throws IOException {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName)
                    .key(objectName)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(stream, stream.available()));
            return true;
        } catch (AmazonServiceException e) {
            logger.error("upload failed,the bucketName is {},the filePath is {}", bucketName, objectName);
            return false;
        }
    }
    public void removeObject(String bucketName, String objectName) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(objectName)
                .build();
        s3Client.deleteObject(deleteObjectRequest);
    }
    public List<S3Object> listBucketObjects(String bucketName, String prefix) {
        List<S3Object> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Request listObjectsRequest =
                ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
        ListObjectsV2Response listing;
        do {
            listing = s3Client.listObjectsV2(listObjectsRequest);
            s3ObjectSummaries.addAll(listing.contents());
            String token = listing.nextContinuationToken();
            listObjectsRequest = listObjectsRequest.toBuilder()
                    .continuationToken(token)
                    .build();
        } while (listing.isTruncated());
        return s3ObjectSummaries;
    }
    public S3Client getAmazonS3() {
        return s3Client;
    }
    public String getBucketName() {
        return ossProperties.getBucketName();
    }

}
