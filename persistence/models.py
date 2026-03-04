"""
ODMantic models for persisting scraped book data from turath.io.

Model hierarchy:
    Author -> BookMetadata -> [BookPage, TableOfContents, ExtractionStats]

Note: In ODMantic 1.x, model inheritance doesn't work properly. Each model
must directly inherit from `Model` instead of using a custom BaseModel.
"""
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from odmantic import Field, Index, Model

if TYPE_CHECKING:
    from odmantic import ObjectId as ObjectIdType


class Author(Model):
    """Author information extracted from turath.io."""
    if TYPE_CHECKING:
        id: "ObjectIdType"

    name: str
    death: str | None = None
    scraped_at: datetime = Field(default_factory=datetime.now)


class BookMetadata(Model):
    """Book metadata extracted from turath.io."""
    if TYPE_CHECKING:
        id: "ObjectIdType"

    ref: str
    title: str
    author: str
    category: str
    size: str
    url: str
    scraped_at: datetime = Field(default_factory=datetime.now)


class BookPage(Model):
    """Individual page content from a book."""
    if TYPE_CHECKING:
        id: "ObjectIdType"

    book_ref: str
    page_id: str
    content: str
    scraped_at: datetime = Field(default_factory=datetime.now)

    model_config: ClassVar[dict] = {
        "indexes": lambda: [
            Index(BookPage.book_ref, BookPage.page_id, unique=True)
        ]
    }


class TocEntry(Model):
    """Single table of contents entry."""
    if TYPE_CHECKING:
        id: "ObjectIdType"

    heading_loc: str
    heading_text: str


class TableOfContents(Model):
    """Complete table of contents for a book."""
    if TYPE_CHECKING:
        id: "ObjectIdType"

    book_ref: str
    entries: list[TocEntry]
    scraped_at: datetime = Field(default_factory=datetime.now)


class ExtractionStats(Model):
    """Extraction statistics and error tracking."""
    if TYPE_CHECKING:
        id: "ObjectIdType"

    book_ref: str
    url: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    pages_extracted: int
    toc_items: int
    metadata: dict[str, Any] | None = None
    author_info: dict[str, Any] | None = None
    errors: list[str] = Field(default_factory=list)