import os
import json
from io import BytesIO
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniopass")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "papers")

_client = None


def get_minio_client():
    """Get or create MinIO client singleton"""
    global _client
    if _client is None:
        _client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        if not _client.bucket_exists(MINIO_BUCKET):
            _client.make_bucket(MINIO_BUCKET)
            print(f"Created MinIO bucket: {MINIO_BUCKET}")
    return _client


def store_paper(task_id, paper):
    """
    Store a paper in MinIO.

    Args:
        task_id: The research task ID
        paper: Paper dict with title, authors, summary, published, source, id

    Returns:
        str: The MinIO object key where paper was stored
    """
    client = get_minio_client()

    source = paper.get("source", "unknown").replace(" ", "_").lower()
    paper_id = paper.get("id", "unknown")
    safe_id = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(paper_id))

    object_key = f"papers/{task_id}/{source}_{safe_id}.json"

    paper_bytes = json.dumps(paper, indent=2, ensure_ascii=False).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        object_key,
        BytesIO(paper_bytes),
        len(paper_bytes),
        content_type="application/json"
    )

    return object_key


def store_papers_batch(task_id, papers):
    """
    Store multiple papers and return their MinIO keys.

    Args:
        task_id: The research task ID
        papers: List of paper dicts

    Returns:
        list: List of MinIO object keys
    """
    refs = []
    for paper in papers:
        try:
            refs.append(store_paper(task_id, paper))
        except S3Error as e:
            print(f"  Warning: Failed to store paper: {e}")
    return refs


def get_paper(object_key):
    """
    Retrieve a paper from MinIO.

    Args:
        object_key: The MinIO object key

    Returns:
        dict: The paper data, or None if not found
    """
    try:
        client = get_minio_client()
        response = client.get_object(MINIO_BUCKET, object_key)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        return data
    except S3Error:
        return None


def get_papers_batch(object_keys):
    """
    Retrieve multiple papers from MinIO.

    Args:
        object_keys: List of MinIO object keys

    Returns:
        list: List of paper dicts (skips any that fail)
    """
    return [p for p in (get_paper(k) for k in object_keys) if p]
