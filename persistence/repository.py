"""
Repository implementations for persisting and querying scraped book data.

Repositories provide CRUD operations and specialized query methods for each model.
"""
from datetime import datetime

from odmantic.bson import ObjectId

from persistence.base import BaseClient, BaseRepository
from persistence.models import (
    Author,
    BookMetadata,
    BookPage,
    ExtractionStats,
    TableOfContents,
)


class AuthorRepository(BaseRepository[Author]):
    """Repository for managing Author entities."""

    def __init__(self, db_client: BaseClient) -> None:
        self.engine = db_client.engine

    async def save(self, author: Author) -> Author:
        """Save an author to the database."""
        return await self.engine.save(author)

    async def get_by_id(self, author_id: str) -> Author | None:
        """Retrieve an author by ID."""
        return await self.engine.find_one(Author, Author.id == ObjectId(author_id))

    async def list_all(self, skip: int = 0, limit: int = 100) -> list[Author]:
        """List all authors with pagination."""
        return await self.engine.find(Author, skip=skip, limit=limit)

    async def delete(self, author_id: str) -> bool:
        """Delete an author by ID."""
        author = await self.get_by_id(author_id)
        if author:
            await self.engine.delete(author)
            return True
        return False

    async def get_by_name(self, name: str) -> Author | None:
        """Retrieve an author by name."""
        return await self.engine.find_one(Author, Author.name == name)

    async def search_by_name(self, name_pattern: str) -> list[Author]:
        """Search authors by name pattern (case-insensitive regex)."""
        return await self.engine.find(
            Author, Author.name.match(f"(?i){name_pattern}")
        )


class BookMetadataRepository(BaseRepository[BookMetadata]):
    """Repository for managing BookMetadata entities."""

    def __init__(self, db_client: BaseClient):
        self.engine = db_client.engine

    async def save(self, book: BookMetadata) -> BookMetadata:
        """Save book metadata to the database."""
        return await self.engine.save(book)

    async def get_by_id(self, book_id: str) -> BookMetadata | None:
        """Retrieve a book by ID."""
        return await self.engine.find_one(BookMetadata, BookMetadata.id == ObjectId(book_id))

    async def list_all(self, skip: int = 0, limit: int = 100) -> list[BookMetadata]:
        """List all books with pagination."""
        return await self.engine.find(BookMetadata, skip=skip, limit=limit)

    async def delete(self, book_id: str) -> bool:
        """Delete a book by ID."""
        book = await self.get_by_id(book_id)
        if book:
            await self.engine.delete(book)
            return True
        return False

    async def get_by_ref(self, ref: str) -> BookMetadata | None:
        """Retrieve a book by its reference number."""
        return await self.engine.find_one(BookMetadata, BookMetadata.ref == ref)

    async def get_by_author(self, author: str) -> list[BookMetadata]:
        """Retrieve all books by a specific author."""
        return await self.engine.find(BookMetadata, BookMetadata.author == author)

    async def get_by_category(self, category: str) -> list[BookMetadata]:
        """Retrieve all books in a specific category."""
        return await self.engine.find(
            BookMetadata, BookMetadata.category == category
        )

    async def search_by_title(self, title_pattern: str) -> list[BookMetadata]:
        """Search books by title pattern (case-insensitive regex)."""
        return await self.engine.find(
            BookMetadata, BookMetadata.title.match(f"(?i){title_pattern}")
        )

    async def get_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> list[BookMetadata]:
        """Retrieve books scraped within a date range."""
        return await self.engine.find(
            BookMetadata,
            (BookMetadata.scraped_at >= start_date)
            & (BookMetadata.scraped_at <= end_date),
        )


class BookPageRepository(BaseRepository[BookPage]):
    """Repository for managing BookPage entities."""

    def __init__(self, db_client: BaseClient):
        self.engine = db_client.engine

    async def save(self, page: BookPage) -> BookPage:
        """Save a book page to the database."""
        return await self.engine.save(page)

    async def get_by_id(self, page_id: str) -> BookPage | None:
        """Retrieve a page by ID."""
        return await self.engine.find_one(BookPage, BookPage.id == ObjectId(page_id))

    async def list_all(self, skip: int = 0, limit: int = 100) -> list[BookPage]:
        """List all pages with pagination."""
        return await self.engine.find(BookPage, skip=skip, limit=limit)

    async def delete(self, page_id: str) -> bool:
        """Delete a page by ID."""
        page = await self.get_by_id(page_id)
        if page:
            await self.engine.delete(page)
            return True
        return False

    async def get_by_book_ref(self, book_ref: str) -> list[BookPage]:
        """Retrieve all pages for a specific book."""
        return await self.engine.find(BookPage, BookPage.book_ref == book_ref)

    async def get_page_by_id(
        self, book_ref: str, page_id: str
    ) -> BookPage | None:
        """Retrieve a specific page by book reference and page ID."""
        return await self.engine.find_one(
            BookPage,
            (BookPage.book_ref == book_ref) & (BookPage.page_id == page_id),
        )

    async def search_content(
        self, book_ref: str, search_term: str
    ) -> list[BookPage]:
        """Search page content within a book (case-insensitive regex)."""
        return await self.engine.find(
            BookPage,
            (BookPage.book_ref == book_ref)
            & BookPage.content.match(f"(?i){search_term}"),
        )

    async def count_pages_for_book(self, book_ref: str) -> int:
        """Count the number of pages for a specific book."""
        pages = await self.get_by_book_ref(book_ref)
        return len(pages)

    async def delete_all_for_book(self, book_ref: str) -> int:
        """Delete all pages for a specific book. Returns count of deleted pages."""
        pages = await self.get_by_book_ref(book_ref)
        deleted_count = 0
        for page in pages:
            await self.engine.delete(page)
            deleted_count += 1
        return deleted_count


class TableOfContentsRepository(BaseRepository[TableOfContents]):
    """Repository for managing TableOfContents entities."""

    def __init__(self, db_client: BaseClient):
        self.engine = db_client.engine

    async def save(self, toc: TableOfContents) -> TableOfContents:
        """Save table of contents to the database."""
        return await self.engine.save(toc)

    async def get_by_id(self, toc_id: str) -> TableOfContents | None:
        """Retrieve TOC by ID."""
        return await self.engine.find_one(TableOfContents, TableOfContents.id == ObjectId(toc_id))

    async def list_all(self, skip: int = 0, limit: int = 100) -> list[TableOfContents]:
        """List all TOCs with pagination."""
        return await self.engine.find(TableOfContents, skip=skip, limit=limit)

    async def delete(self, toc_id: str) -> bool:
        """Delete a TOC by ID."""
        toc = await self.get_by_id(toc_id)
        if toc:
            await self.engine.delete(toc)
            return True
        return False

    async def get_by_book_ref(self, book_ref: str) -> TableOfContents | None:
        """Retrieve TOC for a specific book."""
        return await self.engine.find_one(
            TableOfContents, TableOfContents.book_ref == book_ref
        )

    async def search_entries(
        self, book_ref: str, search_term: str
    ) -> TableOfContents | None:
        """
        Search TOC entries for a specific book.

        Returns the full TOC if any entry matches the search term.
        """
        toc = await self.get_by_book_ref(book_ref)
        if not toc:
            return None

        matching_entries = [
            entry for entry in toc.entries
            if search_term.lower() in entry.heading_text.lower()
        ]

        if matching_entries:
            return toc
        return None


class ExtractionStatsRepository(BaseRepository[ExtractionStats]):
    """Repository for managing ExtractionStats entities."""

    def __init__(self, db_client: BaseClient):
        self.engine = db_client.engine

    async def save(self, stats: ExtractionStats) -> ExtractionStats:
        """Save extraction stats to the database."""
        return await self.engine.save(stats)

    async def get_by_id(self, stats_id: str) -> ExtractionStats | None:
        """Retrieve stats by ID."""
        return await self.engine.find_one(
            ExtractionStats, ExtractionStats.id == ObjectId(stats_id)
        )

    async def list_all(self, skip: int = 0, limit: int = 100) -> list[ExtractionStats]:
        """List all extraction stats with pagination."""
        return await self.engine.find(ExtractionStats, skip=skip, limit=limit)

    async def delete(self, stats_id: str) -> bool:
        """Delete stats by ID."""
        stats = await self.get_by_id(stats_id)
        if stats:
            await self.engine.delete(stats)
            return True
        return False

    async def get_by_book_ref(self, book_ref: str) -> ExtractionStats | None:
        """Retrieve extraction stats for a specific book."""
        return await self.engine.find_one(
            ExtractionStats, ExtractionStats.book_ref == book_ref
        )

    async def get_failed_extractions(self) -> list[ExtractionStats]:
        """Retrieve all extraction stats that have errors."""
        all_stats = await self.list_all(limit=1000)
        return [stats for stats in all_stats if stats.errors]

    async def get_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> list[ExtractionStats]:
        """Retrieve extraction stats within a date range."""
        return await self.engine.find(
            ExtractionStats,
            (ExtractionStats.start_time >= start_date)
            & (ExtractionStats.start_time <= end_date),
        )

    async def get_average_duration(self) -> float | None:
        """Calculate average extraction duration across all stats."""
        all_stats = await self.list_all(limit=1000)
        if not all_stats:
            return None
        durations = [s.duration_seconds for s in all_stats if s.duration_seconds]
        return sum(durations) / len(durations) if durations else None