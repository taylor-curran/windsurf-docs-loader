# src/loaders/models.py

from typing import Optional
from pydantic import BaseModel


class DocsPage(BaseModel):
    url: str
    title: str
    tool: str
    content: str
    unique_id: Optional[str] = None
    proprietary: Optional[bool] = None
