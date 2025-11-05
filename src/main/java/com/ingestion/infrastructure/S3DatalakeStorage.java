package com.ingestion.infrastructure;

import com.ingestion.application.ports.DatalakeStorage;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class S3DatalakeStorage implements DatalakeStorage {

    private final S3Client s3;
    private final String bucket;
    private final String basePrefix;

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter HOUR_FMT = DateTimeFormatter.ofPattern("HH");

    public S3DatalakeStorage(S3Client client, String bucket, String prefix) {
        this.s3 = client;
        this.bucket = bucket;
        this.basePrefix = prefix;
        ensureBucketExists(this.bucket);
    }


    @Override
    public void saveBook(int bookId, Path stagingDir, LocalDateTime timestamp) throws IOException {
        Path bodySrc   = stagingDir.resolve(bookId + "_body.txt");
        Path headerSrc = stagingDir.resolve(bookId + "_header.txt");

        if (!Files.exists(bodySrc) || !Files.exists(headerSrc)) {
            throw new IOException("Missing source files for book " + bookId + " at " + stagingDir.toAbsolutePath());
        }

        try {
            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(bodyKey(bookId, timestamp))
                            .contentType("text/plain; charset=utf-8")
                            .build(),
                    RequestBody.fromFile(bodySrc));

            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(headerKey(bookId, timestamp))
                            .contentType("text/plain; charset=utf-8")
                            .build(),
                    RequestBody.fromFile(headerSrc));

            Files.deleteIfExists(bodySrc);
            Files.deleteIfExists(headerSrc);
            System.out.println("[INFO] Book " + bookId + " uploaded to s3://" + bucket + "/" + folderFor(timestamp));
        } catch (S3Exception e) {
            throw new IOException("Error uploading book " + bookId + " to S3: " + safeAwsMsg(e), e);
        }
    }

    @Override
    public boolean exists(int bookId) throws IOException {
        String searchPrefix = basePrefix + "datalake/";
        String needle = "/" + bookId + ".body.txt";
        try {
            String token = null;
            do {
                ListObjectsV2Response resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(searchPrefix)
                        .continuationToken(token)
                        .maxKeys(1000)
                        .build());
                for (S3Object obj : resp.contents()) {
                    if (obj.key().endsWith(needle)) return true;
                }
                token = resp.isTruncated() ? resp.nextContinuationToken() : null;
            } while (token != null);
            return false;
        } catch (S3Exception e) {
            throw new IOException("Error listing S3 objects for exists(" + bookId + "): " + safeAwsMsg(e), e);
        }
    }

    @Override
    public List<Integer> listBooks() throws IOException {
        String searchPrefix = basePrefix + "datalake/";
        try {
            Set<Integer> ids = new HashSet<>();
            String token = null;
            do {
                ListObjectsV2Response resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(searchPrefix)
                        .continuationToken(token)
                        .maxKeys(1000)
                        .build());
                for (S3Object obj : resp.contents()) {
                    String key = obj.key();
                    if (key.endsWith(".body.txt")) {
                        int slash = key.lastIndexOf('/');
                        String file = (slash >= 0) ? key.substring(slash + 1) : key;
                        int dot = file.indexOf('.');
                        if (dot > 0) {
                            String idStr = file.substring(0, dot);
                            try { ids.add(Integer.parseInt(idStr)); } catch (NumberFormatException ignored) {}
                        }
                    }
                }
                token = resp.isTruncated() ? resp.nextContinuationToken() : null;
            } while (token != null);
            return ids.stream().sorted().collect(Collectors.toList());
        } catch (S3Exception e) {
            throw new IOException("Error listing S3 objects for listBooks(): " + safeAwsMsg(e), e);
        }
    }

    @Override
    public String relativePathFor(int bookId, LocalDateTime timestamp) {
        return "datalake/" + day(timestamp) + "/" + hour(timestamp) + "/" + bookId;
    }

    private void ensureBucketExists(String bucket) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (S3Exception e) {
            try {
                s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
                System.out.println("[CONF] Created bucket: " + bucket);
            } catch (S3Exception ce) {
                String code = safeAwsCode(ce);
                if (!"BucketAlreadyOwnedByYou".equals(code) && !"BucketAlreadyExists".equals(code)) {
                    throw ce;
                }
            }
        }
    }

    private String day(LocalDateTime ts)  { return ts.format(DATE_FMT); }
    private String hour(LocalDateTime ts) { return ts.format(HOUR_FMT); }

    private String folderFor(LocalDateTime ts) {
        return basePrefix + "datalake/" + day(ts) + "/" + hour(ts) + "/";
    }

    private String bodyKey(int bookId, LocalDateTime ts)   { return folderFor(ts) + bookId + ".body.txt"; }
    private String headerKey(int bookId, LocalDateTime ts) { return folderFor(ts) + bookId + ".header.txt"; }

    private static String safeAwsMsg(S3Exception e) {
        try { return e.awsErrorDetails().errorCode() + ": " + e.awsErrorDetails().errorMessage(); }
        catch (Exception ignore) { return e.getMessage(); }
    }

    private static String safeAwsCode(S3Exception e) {
        try { return e.awsErrorDetails().errorCode(); } catch (Exception ignore) { return null; }
    }
}
