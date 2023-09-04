from google.cloud import storage
from typing import Any, Optional, Sequence, Union
from airflow.models import BaseOperator
import io
import os
import requests
import tempfile
import pandas as pd

class WebToGCSOperator(BaseOperator):
    """"
    move data from website to a GCS bucket
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "service",
        "destination_path",
        "destination_bucket",
    )


    def __init__(
            self,
            *,
            endpoint: str,
            destination_path:  Optional[str] = None,
            destination_bucket:  Optional[str] = None,
            service: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.endpoint = self._format_endpoint(endpoint,service,destination_path)
        self.destination_path = self._format_destination_path(destination_path)
        self.destination_bucket = self._format_bucket_name(destination_bucket)
        self.service = service

    def execute(self, context: Any):
        self._copy_file_object()


    def _copy_file_object(self) -> None:

        """function to download and copy file into gcs bucket """

        self.log.info("Execute downloading of file from %s to gs://%s//%s",
                    self.endpoint, # url
                    self.destination_bucket, # bucket name
                    self.destination_path #yellow/yellow_tripdata_2021-01.csv.gz
        )

        # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # (Ref: https://github.com/googleapis/python-storage/issues/74)
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        # End of Workaround


        # download it using requests via into a tempfile a pandas df
        with tempfile.TemporaryDirectory() as tmpdirname:
                request_url = self.endpoint
                # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

                r = requests.get(request_url)


                open(f'{tmpdirname}/{self.destination_path}', 'wb').write(r.content)
                self.log.info(f"File written to temporary directory: {tmpdirname}/{self.destination_path}")

                # read it back into a parquet file
                file_name1=f'{tmpdirname}/{self.destination_path}'
                file_name1 = file_name1.replace('.csv.gz', '.csv')
                open(file_name1, 'w').write(f'{tmpdirname}/{self.destination_path}')
                df = pd.read_csv(file_name1, encoding="utf-8")
                
                file_name=self.destination_path
                file_name = file_name.replace('.csv.gz', '.parquet')

                # yellow_tripdata_2021-01.parquet
                df.to_parquet(f'{tmpdirname}/{file_name}', engine='pyarrow')
                self.log.info(f"Parquet: {file_name}")

                # upload it to gcs
                client = storage.Client()
                bucket = client.bucket(self.destination_bucket)

                blob = bucket.blob(f"{self.service}/{file_name}")
                blob.upload_from_filename(f'{tmpdirname}/{file_name}')
                # upload_to_gcs(BUCKET, f"/yellow/2020/yellow_tripdata_2021-01.parquet", f'{tmpdirname}/{file_name}')

                self.log.info("Loaded file from %s to gs://%s//%s",
                    self.endpoint,
                    self.destination_bucket,
                    f"{self.service}/{file_name}"
                )

    @staticmethod
    def _format_endpoint(endpoint: Optional[str], service: str, destination_path: str) -> str:
        endpoint = (f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{destination_path}"
        )
        return endpoint

    @staticmethod
    def _format_destination_path(path: Union[str,None]) -> str:
         if path is not None:
              return path.lstrip("/") if path.startswith("/") else path
         return ""

    @staticmethod
    def _format_bucket_name(name: str) -> str:
         bucket =  name if not name.startswith("gs://") else name[5:]
         return bucket.strip("/")
