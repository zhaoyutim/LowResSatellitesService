import os

if __name__=='__main__':
    def implicit():
        from google.cloud import storage

        # If you don't specify credentials when constructing the client, the
        # client library will look for credentials in the environment.
        storage_client = storage.Client()

        # Make an authenticated API request
        buckets = list(storage_client.list_buckets())
        print(buckets)
        bucket = storage_client.bucket('ai4wildfire')
        print(storage.Blob(bucket=bucket, name='VNPIMGTIF/2021-07-14/VNPIMGA20211950718.tif').exists(storage_client))
        os.system('gsutil cp data/cogtif/2020-08-15/VNPIMG2020-08-15-0806.tif gs://ai4wildfire/VNPIMGTIF/20202020-08-15/VNPIMG2020-08-15-0806.tif')
    implicit()