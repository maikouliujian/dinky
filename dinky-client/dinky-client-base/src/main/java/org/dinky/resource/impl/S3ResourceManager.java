/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.resource.impl;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import org.dinky.data.enums.Status;
import org.dinky.data.exception.BusException;
import org.dinky.data.model.ResourcesVO;
import org.dinky.resource.BaseResourceManager;
import org.dinky.s3.S3Template;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3ResourceManager implements BaseResourceManager {
    private S3Template s3Template;
    private String removeFirstSlash(String path) {
        if (path.startsWith("/")){
            return path.substring(1);
        } else {
            return path;
        }
    }
    @Override
    public void remove(String path) {
        getS3Template().removeObject(getS3Template().getBucketName(), removeFirstSlash(getFilePath(path)));
    }

    @Override
    public void rename(String path, String newPath) {
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(getS3Template().getBucketName())
                .sourceKey(removeFirstSlash(getFilePath(path)))
                .destinationBucket(getS3Template().getBucketName())
                .destinationKey(removeFirstSlash(getFilePath(newPath)))
                .build();
        getS3Template().getAmazonS3().copyObject(copyReq);
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(getS3Template().getBucketName())
                .key(removeFirstSlash(getFilePath(path)))
                .build();
        getS3Template().getAmazonS3().deleteObject(deleteObjectRequest);
    }

    @Override
    public void putFile(String path, InputStream fileStream) {
        try {
            getS3Template().putObject(getS3Template().getBucketName(), removeFirstSlash(getFilePath(path)), fileStream);
        } catch (Exception e) {
            throw new BusException(Status.RESOURCE_FILE_UPLOAD_FAILED, e);
        }
    }
    @Override
    public void putFile(String path, File file) {
        try {
            getS3Template()
                    .putObject(getS3Template().getBucketName(), removeFirstSlash(getFilePath(path)), FileUtil.getInputStream(file));
        } catch (Exception e) {
            throw new BusException(Status.RESOURCE_FILE_UPLOAD_FAILED, e);
        }
    }

    @Override
    public String getFileContent(String path) {
        return IoUtil.readUtf8(readFile(path));
    }

    @Override
    public List<ResourcesVO> getFullDirectoryStructure(int rootId) {
        String basePath = getBasePath();
        if (StrUtil.isNotBlank(basePath)) {
            if (basePath.charAt(0) == '/') {
                basePath = basePath.substring(1);
            }
        }

        List<S3Object> listBucketObjects =
                getS3Template().listBucketObjects(getS3Template().getBucketName(), basePath);
        Map<Integer, ResourcesVO> resourcesMap = new HashMap<>();

        for (S3Object obj : listBucketObjects) {
            String key = obj.key().replace(basePath, "");
            if (key.isEmpty()) {
                continue;
            }
            String[] split = key.split("/");
            String parent = "";
            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                int pid = parent.isEmpty() ? rootId : parent.hashCode();
                parent = parent + "/" + s;
                ResourcesVO.ResourcesVOBuilder builder = ResourcesVO.builder()
                        .id(parent.hashCode())
                        .pid(pid)
                        .fullName(parent)
                        .fileName(s)
                        .isDirectory(key.endsWith("/"))
                        .size(obj.size());
                if (i == split.length - 1) {
                    builder.isDirectory(key.endsWith("/"));
                } else {
                    builder.isDirectory(true);
                }
                resourcesMap.put(parent.hashCode(), builder.build());
            }
        }
        return new ArrayList<>(resourcesMap.values());
    }

    @Override
    public InputStream readFile(String path) {
        return getS3Template()
                .getObject(getS3Template().getBucketName(), removeFirstSlash(path));
    }

    public S3Template getS3Template() {
        if (s3Template == null && instances.getResourcesEnable().getValue()) {
            throw new BusException(Status.RESOURCE_OSS_CONFIGURATION_ERROR);
        }
        return s3Template;
    }

    public void setS3Template(S3Template s3Template) {
        this.s3Template = s3Template;
    }
}
