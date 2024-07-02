package com.onthegomap.planetiler.util;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.messages.DeleteObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinioUtils {

  private final Logger logger = LoggerFactory.getLogger(MinioUtils.class);
  private MinioClient minioClient;
  private String bucketName;
  private String endpoint;


  public MinioUtils() {
    init();
  }

  public void init() {
    try (InputStream input = MinioUtils.class.getClassLoader().getResourceAsStream("minio.properties")) {
      if (input == null) {
        logger.error("Sorry, unable to find minio.properties");
      }

      Properties prop = new Properties();
      prop.load(input);

      String endpoint = prop.getProperty("minio.endpoint");
      String accessKey = prop.getProperty("minio.accessKey");
      String secretKey = prop.getProperty("minio.secretKey");
      bucketName = prop.getProperty("minio.bucketName");

      minioClient = MinioClient.builder()
        .endpoint(endpoint)
        .credentials(accessKey, secretKey)
        .build();

      if (!bucketExists(bucketName)) {
        makeBucket(bucketName);
      }
    } catch (Exception ex) {
      logger.error("Error occurred while initializing MinIO client", ex);
    }
  }


  public MinioUtils(String endpoint, String accessKey, String secretKey, String bucketName) {
    this.minioClient = MinioClient.builder()
      .endpoint(endpoint)
      .credentials(accessKey, secretKey)
      .build();
    this.bucketName = bucketName;
    this.endpoint = endpoint;
  }

  /**
   * 存储桶是否存在
   *
   * @param bucketName 存储桶名称
   * @return boolean
   */
  public boolean bucketExists(String bucketName) {
    try {
      return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
    } catch (Exception e) {
      logger.error("minio bucketExists Exception:{}", e);
    }
    return false;
  }

  /**
   * @Description: 创建 存储桶
   * @Param bucketName: 存储桶名称
   * @return: void
   */
  public void makeBucket(String bucketName) {
    try {
      if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
      }
    } catch (Exception e) {
      logger.error("minio makeBucket Exception:{}", e);
    }
  }

  /**
   * 上传文件
   *
   * @param objectName 存储桶对象名称
   * @param stream     文件流
   * @return BladeFile
   */
  public boolean upLoadFile(String objectName, InputStream stream) {
    upLoadFile(bucketName, objectName, stream, "application/octet" + "-stream");
    return true;
  }

  /**
   * @Description: 上传文件
   * @Param bucketName: 存储桶名称
   * @Param folderName: 上传的文件夹名称
   * @Param fileName: 上传文件名
   * @Param suffix: 文件后缀名
   * @Param stream: 文件流
   * @Param contentType: 文件类型
   */
  public boolean upLoadFile(String bucketName, String objectName, InputStream stream,
    String contentType) {
    try {
      minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(objectName)
        .stream(stream, stream.available(), -1).contentType(contentType).build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        logger.error("文件流关闭失败");
      }
    }

    return true;
  }

  /**
   * 删除文件
   *
   * @param fileName 存储桶对象名称
   */
  public boolean removeFile(String fileName) {
    try {
      minioClient.removeObject(
        RemoveObjectArgs.builder().bucket(bucketName).object(fileName)
          .build());
      logger.info("minio removeFile success, fileName:{}", fileName);
      return true;
    } catch (Exception e) {
      logger.error("minio removeFile fail, fileName:{}, Exception:{}", fileName, e);
    }
    return false;
  }

  /**
   * 批量删除文件
   *
   * @param fileNames 存储桶对象名称集合
   */
  public boolean removeFiles(List<String> fileNames) {
    try {
      Stream<DeleteObject> stream = fileNames.stream().map(DeleteObject::new);
      minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName)
        .objects(stream::iterator).build());
      logger.info("minio removeFiles success, fileNames:{}", fileNames);
      return true;
    } catch (Exception e) {
      logger.error("minio removeFiles fail, fileNames:{}, Exception:{}", fileNames, e);
    }
    return false;
  }

  public String getEndpoint() {
    return this.endpoint;
  }

  public String getBucketName() {
    return this.bucketName;
  }
}
