import datetime
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
from pymongo import ASCENDING
from typing import Any

class MongoCache:
    def __init__(self, mongo_url: str, db_name: str = "muzza", bucket_name: str = "cache"):
        self.client = AsyncIOMotorClient(mongo_url)
        self.db = self.client[db_name]
        self.fs = AsyncIOMotorGridFSBucket(self.db, bucket_name=bucket_name)
        self.meta = self.db[f"{bucket_name}_meta"]

    async def setup_ttl_index(self, expire_seconds: int = 86400):
        await self.meta.create_index(
            [("createdAt", ASCENDING)],
            expireAfterSeconds=expire_seconds
        )

    async def save_file(self, key: str, data: bytes, meta: dict[str, Any]):
        file_id = await self.fs.upload_from_stream(key, data)
        meta_doc = {"key": key, "file_id": file_id, "createdAt": datetime.datetime.utcnow(), **meta}
        await self.meta.replace_one({"key": key}, meta_doc, upsert=True)
        return file_id

    async def get_file(self, key: str):
        doc = await self.meta.find_one({"key": key})
        if not doc:
            return None
        grid_out = await self.fs.open_download_stream(doc["file_id"])
        data = await grid_out.read()
        return data, doc

    async def delete_file(self, key: str):
        doc = await self.meta.find_one({"key": key})
        if doc:
            await self.fs.delete(doc["file_id"])
            await self.meta.delete_one({"key": key})

    async def clear(self):
        await self.meta.delete_many({})
        # GridFS files will be deleted by TTL index automatically