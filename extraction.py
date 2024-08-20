import boto3
import tarfile
import io

# Initialize S3 client
s3 = boto3.client('s3', region_name='eu-central-1')

# Define source and destination
source_bucket = 'aev-autonomous-driving-dataset'
source_object = 'camera_lidar_semantic.tar'
destination_bucket = 'lntav'
destination_prefix = 'Dataset/'

# Stream the tar file and extract its contents on-the-fly
response = s3.get_object(Bucket=source_bucket, Key=source_object)
tar_stream = tarfile.open(fileobj=response['Body'], mode='r|*')

def upload_to_s3(bucket, key, data):
    """Upload data to S3."""
    s3.put_object(Bucket=bucket, Key=key, Body=data)

for member in tar_stream:
    if member.isfile():  # Ensure it's a file and not a directory
        # Extract file content as a stream
        file_obj = tar_stream.extractfile(member)
        if file_obj is not None:
            destination_key = f"{destination_prefix}{member.name}"
            
            # Read the content from the file_obj in chunks and upload
            chunk_size = 1024 * 1024  # 1 MB chunks
            chunk = file_obj.read(chunk_size)
            while chunk:
                upload_to_s3(destination_bucket, destination_key, chunk)
                chunk = file_obj.read(chunk_size)
            
            print(f"Uploaded {member.name} to s3://{destination_bucket}/{destination_key}")

print("Transfer complete.")
