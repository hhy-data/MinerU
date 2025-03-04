from minio import Minio
from magic_pdf.data.io.base import IOReader, IOWriter
import io


class MinioReader(IOReader):
    def __init__(
        self,
        bucket: str,
        ak: str,
        sk: str,
        endpoint_url: str,
        addressing_style: str = 'auto',
    ):
        """s3 reader client.

        Args:
            bucket (str): bucket name
            ak (str): access key
            sk (str): secret key
            endpoint_url (str): endpoint url of s3
            addressing_style (str, optional): Defaults to 'auto'. Other valid options here are 'path' and 'virtual'
            refer to https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3.html
        """
        self._bucket = bucket
        self._ak = ak
        self._sk = sk
        self._s3_client = Minio(
            access_key=ak,
            secret_key=sk,
            endpoint=endpoint_url,
            secure=False,
        )

    def read(self, key: str) -> bytes:
        """Read the file.

        Args:
            path (str): file path to read

        Returns:
            bytes: the content of the file
        """
        return self.read_at(key)

    def read_at(self, key: str, offset: int = 0, limit: int = 0) -> bytes:
        """Read at offset and limit.

        Args:
            path (str): the path of file, if the path is relative path, it will be joined with parent_dir.
            offset (int, optional): the number of bytes skipped. Defaults to 0.
            limit (int, optional): the length of bytes want to read. Defaults to -1.

        Returns:
            bytes: the content of file
        """
        response = self._s3_client.get_object(
            bucket_name=self._bucket, object_name=key, offset=offset, length=limit,
        )
        data = response.read(limit)
        response.close()
        response.release_conn()
        return data


class MinioWriter(IOWriter):
    def __init__(
        self,
        bucket: str,
        ak: str,
        sk: str,
        endpoint_url: str,
        addressing_style: str = 'auto',
    ):
        """s3 reader client.

        Args:
            bucket (str): bucket name
            ak (str): access key
            sk (str): secret key
            endpoint_url (str): endpoint url of s3
            addressing_style (str, optional): Defaults to 'auto'. Other valid options here are 'path' and 'virtual'
            refer to https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3.html
        """
        self._bucket = bucket
        self._ak = ak
        self._sk = sk
        self._s3_client = Minio(
            access_key=ak,
            secret_key=sk,
            endpoint=endpoint_url,
            secure=False,
        )

    def write(self, key: str, data: bytes):
        """Write file with data.

        Args:
            path (str): the path of file, if the path is relative path, it will be joined with parent_dir.
            data (bytes): the data want to write
        """
        data_stream = io.BytesIO(data)

        self._s3_client.put_object(
            bucket_name=self._bucket,
            object_name=key,
            data=data_stream,
            length=len(data),
            content_type="application/octet-stream")
