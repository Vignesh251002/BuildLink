// index.mjs
import {
  S3Client,
  PutObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const s3Client = new S3Client({ region: process.env.AWS_REGION });
const BUCKET = process.env.S3_BUCKET_NAME;

// Max single upload size ~5MB (testing purposes - adjust as needed)
const SINGLE_UPLOAD_LIMIT = 5 * 1024 * 1024;

// Lambda handler
export const handler = async (event) => {
  let body = {};
  try {
    body = event.body ? JSON.parse(event.body) : {};
  } catch {
    return formatResponse(400, { error: "Invalid JSON body" });
  }

  const { fileName, contentType, fileSize, uploadId, complete, parts } = body;

  if (!fileName || !contentType || !fileSize) {
    return formatResponse(400, { error: "Missing 'fileName', 'contentType' or 'fileSize'" });
  }

  try {
    const uploadType = chooseUploadType(fileSize);

    // SINGLE FILE
    if (uploadType === "single") {
      const response = await getPresignedUrl({ fileName, contentType });
      return formatResponse(200, response);
    }

    // MULTIPART
    if (uploadType === "multipart") {
      // Step 1: Start multipart and generate presigned URLs
      if (!uploadId) {
        const response = await startMultipart({ fileName, contentType, fileSize });
        return formatResponse(200, response);
      }

      // Step 2: Complete multipart
      if (uploadId && complete && parts) {
        const response = await completeMultipart({ fileName, uploadId, parts });
        return formatResponse(200, response);
      }

      return formatResponse(400, { error: "Invalid multipart request. Make sure to provide 'uploadId', 'complete', and 'parts' when completing multipart upload." });
    }

    return formatResponse(400, { error: "Invalid request flow" });
  } catch (err) {
    console.error("Lambda error:", err);
    return formatResponse(500, { error: err.message });
  }
};

function chooseUploadType(fileSize) {
  if (!fileSize) return "single";
  return fileSize <= SINGLE_UPLOAD_LIMIT ? "single" : "multipart";
}

// Generate presigned URL for single upload
async function getPresignedUrl({ fileName, contentType }) {
  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: fileName,
    ContentType: contentType ,
  });

  const url = await getSignedUrl(s3Client, command, { expiresIn: 900 });
  return { uploadType: "single", fileName, contentType, url };
}

// Start multipart and generate presigned URLs
async function startMultipart({ fileName, contentType, fileSize }) {
  const totalParts = Math.ceil(fileSize / SINGLE_UPLOAD_LIMIT);

  const createCommand = new CreateMultipartUploadCommand({
    Bucket: BUCKET,
    Key: fileName,
    ContentType: contentType ,
  });

  const { UploadId } = await s3Client.send(createCommand);

  const urls = [];
  for (let partNumber = 1; partNumber <= totalParts; partNumber++) {
    const uploadPartCommand = new UploadPartCommand({
      Bucket: BUCKET,
      Key: fileName,
      UploadId,
      PartNumber: partNumber,
    });
    const url = await getSignedUrl(s3Client, uploadPartCommand, { expiresIn: 3600 });
    urls.push({ partNumber, url });
  }

  return { uploadType: "multipart", fileName, contentType, uploadId: UploadId, totalParts, urls };
}

// Complete multipart
async function completeMultipart({ fileName, uploadId, parts }) {
  const command = new CompleteMultipartUploadCommand({
    Bucket: BUCKET,
    Key: fileName,
    UploadId: uploadId,
    MultipartUpload: { Parts: parts },
  });
  const result = await s3Client.send(command);
  return { uploadType: "multipart", fileName, result };
}


// Format Lambda response
function formatResponse(statusCode, body) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}
