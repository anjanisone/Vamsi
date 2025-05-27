from pyspark.sql.functions import concat_ws
import requests
import json

df = spark.read.table("your_catalog.your_schema.your_table")

df = df.withColumn("full_path", concat_ws("\\\\",
    df["carepro_RootFolderPath"],
    df["carepro_ContainerFolders"],
    df["carepro_DocumentBase_Annotation_carepro_FileName"]
))

df_selected = df.select(
    df["full_path"],
    df["carepro_AuthrequestId"].alias("auth_request_id"),
    df["carepro_DocumentBase_Annotation_Id"].alias("annotation_id")
)

payload = [row.asDict() for row in df_selected.collect()]

response = requests.post(
    url="https://<your-function-app>.azurewebsites.net/api/httpstart",
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload)
)

print(response.status_code)
print(response.text)