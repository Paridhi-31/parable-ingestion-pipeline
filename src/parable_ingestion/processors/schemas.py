from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from bson import ObjectId

class AuthorSchema(BaseModel):
    name: str
    slug: str
    bio: str = ""
    profilePicture: str = ""
    nationality: str = ""
    createdAt: datetime = Field(default_factory=datetime.utcnow)

class ChapterSchema(BaseModel):
    book: str # Book OID
    title: str
    order: int
    contentUrl: Optional[str] = None
    audioFileUrl: Optional[str] = None

class BookSchema(BaseModel):
    title: str
    slug: str
    description: str = ""
    coverImage: str = ""
    author: str # Author OID
    genre: List[str] # List of Genre OIDs
    chapters: List[str] = [] # List of Chapter OIDs
    ebookFileUrl: str
    ebookFileType: str = "epub"
    isPremium: bool = False
    price: float = 0.0
    hasAudiobook: bool = False