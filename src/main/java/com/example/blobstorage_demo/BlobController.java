package com.example.blobstorage_demo;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

@RestController
@RequestMapping("blob")
public class BlobController {

  @Value("azure-blob://test/testFile.txt")
  private Resource blobFile;

  /**
   * Azure Storageのアカウント名
   */
  @Value("${spring.cloud.azure.storage.blob.account-name}")
  private String storageAccountName;

  /**
   * Azure Storageへのアクセスキー
   */
  @Value("${spring.cloud.azure.storage.blob.account-key}")
  private String storageAccessKey;

  /**
   * Azure StorageのBlobコンテナー名
   */
  @Value("test")
  private String storageContainerName;

  private static final int csvRowSize = 1000;
  private static final int csvColumnSize = 100;


  @GetMapping("/readBlobFile")
  public String readBlobFile() throws IOException {
    return StreamUtils.copyToString(
        this.blobFile.getInputStream(), Charset.defaultCharset());
  }

  @PostMapping("/writeBlobFile")
  public String writeBlobFile(@RequestBody String data) throws IOException {
    try (OutputStream os = ((WritableResource) this.blobFile).getOutputStream()) {
      os.write(data.getBytes());
    }
    return "file was updated";
  }

  @GetMapping("/generateAndUpload")
  public String generateAndUploadFile() {
    try {
      // Blobストレージへの接続文字列
      String storageConnectionString = "DefaultEndpointsProtocol=https;"
          + "AccountName=" + storageAccountName + ";"
          + "AccountKey=" + storageAccessKey + ";";

      // ストレージアカウントオブジェクトを取得
      CloudStorageAccount storageAccount
          = CloudStorageAccount.parse(storageConnectionString);

      // Blobクライアントオブジェクトを取得
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

      // Blob内のコンテナーを取得
      CloudBlobContainer container
          = blobClient.getContainerReference(storageContainerName);

      CloudBlockBlob blob = container.getBlockBlobReference("test.csv");
      blob.getProperties().setContentType("text/csv");

      BlobOutputStream blobOutputStream = blob.openOutputStream();


      for (int i = 0; i < csvRowSize; i++) {
        for (int j = 0; j < csvColumnSize; j++) {
          blobOutputStream.write((Integer.valueOf(i + j).toString()).getBytes(StandardCharsets.UTF_8));
          if (j != csvColumnSize - 1) {
            blobOutputStream.write(",".getBytes(StandardCharsets.UTF_8));
          }
        }
        blobOutputStream.write(System.getProperty("line.separator").getBytes(StandardCharsets.UTF_8));
      }
      blobOutputStream.flush();
      blobOutputStream.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return "generate and upload finish";
  }

  @GetMapping("/getFileSize")
  public String getFileSize() {

    try {
      // Blobストレージへの接続文字列
      String storageConnectionString = "DefaultEndpointsProtocol=https;"
          + "AccountName=" + storageAccountName + ";"
          + "AccountKey=" + storageAccessKey + ";";

      // ストレージアカウントオブジェクトを取得
      CloudStorageAccount storageAccount
          = CloudStorageAccount.parse(storageConnectionString);

      // Blobクライアントオブジェクトを取得
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

      // Blob内のコンテナーを取得
      CloudBlobContainer container
          = blobClient.getContainerReference(storageContainerName);

      CloudBlockBlob blob = container.getBlockBlobReference("test.csv");
      blob.downloadAttributes();
      return String.valueOf(blob.getProperties().getLength());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


}
