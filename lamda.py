import etl
import boto3
import io

ny_url = "data/ETL_data.csv"
jh_url = "data/ETL_recovery.csv"

ACCESS_KEY_ID = 'YOUR_ACCESS_KEY_ID'
SECRET_ACCESS_KEY = 'YOUR_SECRET_ACCESS_ID'

filename = 'MyPandasData.'
bucketName = 'mypyfile'
s3_client = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,

)

df_final = etl.extract_transform(ny_url, jh_url)
with io.StringIO() as csv_buffer:
    df_final.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=bucketName, Key="files/covid_final.csv", Body=csv_buffer.getvalue()
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")



