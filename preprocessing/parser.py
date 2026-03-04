"""
Text parser for Arabic book pages that separates body text from footnotes.

The parser identifies footnotes by lines starting with Arabic-Indic numerals
in parentheses format like (١), (٢), etc.

Design Patterns Used:
- Strategy Pattern: Configurable parsing strategies via FootnoteStrategy
- Factory Pattern: DocumentParserFactory for creating parsers with different configs
- Repository Pattern: DocumentRepository for file I/O operations
"""
# TODO: Adjust the footnote extraction logic to handle the following case: ✓
"""
## Proposed Rule Logic:
1. Identify all markers: Scan the entire text for all occurrences of lines starting with (١), (٢), (٣), etc.
2. Count occurrences: For each marker, count how many times it appears in the text.
3. Determine footnote start:
   - If a marker appears at least twice, the second occurrence is considered the start of the footnote section.
   - If a marker appears only once, ignore it (it's likely part of the body).
4. Split the text: Once the first valid footnote marker is found, split the text into body and footnote sections.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Protocol, runtime_checkable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class Document:
    """Represents a parsed document with body and footnotes."""
    source_path: Path
    body: str
    footnotes: str
    metadata: dict = field(default_factory=dict)

    @property
    def has_footnotes(self) -> bool:
        """Check if document contains footnotes."""
        return bool(self.footnotes.strip())

    @property
    def word_count(self) -> int:
        """Get total word count of body text."""
        return len(self.body.split())

    def to_markdown(self) -> str:
        """Convert document to Markdown format."""
        markdown = self.body
        if self.has_footnotes:
            markdown += f"\n\n---\n\n## Footnotes\n\n{self.footnotes}"
        return markdown


class ProcessingStatus(Enum):
    """Status of document processing."""
    PENDING = auto()
    SUCCESS = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass
class ProcessingResult:
    """Result of processing a single document."""
    document: Document | None
    status: ProcessingStatus
    error: str | None = None
    output_path: Path | None = None


@runtime_checkable
class FootnoteStrategy(Protocol):
    """Protocol for footnote detection strategies."""

    def is_footnote_line(self, line: str) -> bool:
        """Check if a line is a footnote."""
        ...


class ArabicIndicFootnoteStrategy:
    """
    Default strategy: detects Arabic-Indic numerals in parentheses.

    Implements the proposed rule logic:
    - Markers appearing only once are considered body references
    - Markers appearing 2+ times: second occurrence starts footnotes section
    """

    def __init__(self):
        # Pattern for footnote markers at start of line: (١), (٢), etc.
        self.pattern = re.compile(r'^\s*\([٠١٢٣٤٥٦٧٨٩]+\)')
        # Pattern to extract marker number: (١), (٢), etc.
        self._marker_pattern = re.compile(r'\(([٠١٢٣٤٥٦٧٨٩]+)\)')
        self._logger = logging.getLogger(f"{__name__}.ArabicIndicFootnoteStrategy")

    def is_footnote_line(self, line: str) -> bool:
        """Check if line starts with Arabic-Indic footnote marker."""
        return bool(self.pattern.match(line))

    def extract_marker(self, line: str) -> str | None:
        """Extract the marker number from a line (e.g., '١' from '(١)')."""
        match = self._marker_pattern.match(line.lstrip())
        return match.group(1) if match else None

    def find_all_markers_in_line(self, line: str) -> list[str]:
        """Find all footnote markers in a line (for inline references)."""
        return self._marker_pattern.findall(line)

    def find_footnote_start_index(self, lines: list[str]) -> int:
        """
        Find the index where footnotes section begins.

        Algorithm:
        1. Count ALL occurrences of each marker (both inline and line-starting)
        2. Track seen markers as we iterate through lines (all occurrences)
        3. Return index of first LINE-STARTING marker that appears for the 2nd time overall

        Returns:
            Index of first footnote line, or -1 if no footnotes detected
        """
        # First pass: count ALL marker occurrences (inline + line-starting)
        marker_counts: dict[str, int] = {}
        for line in lines:
            markers = self.find_all_markers_in_line(line)
            for marker in markers:
                marker_counts[marker] = marker_counts.get(marker, 0) + 1

        self._logger.debug(f"Marker counts: {marker_counts}")

        # Second pass: track all occurrences and find first line-starting 2nd occurrence
        marker_seen_count: dict[str, int] = {}
        for i, line in enumerate(lines):
            # Count all markers in this line (inline or line-starting)
            markers_in_line = self.find_all_markers_in_line(line)
            for marker in markers_in_line:
                marker_seen_count[marker] = marker_seen_count.get(marker, 0) + 1

            # Check if this line starts with a marker that is now at its 2nd occurrence
            if self.is_footnote_line(line):
                marker = self.extract_marker(line)
                if marker and marker_seen_count.get(marker, 0) >= 2:
                    self._logger.info(
                        f"Footnotes start at line {i}: marker ({marker}) "
                        f"appears {marker_counts[marker]} times total"
                    )
                    return i

        self._logger.debug(f"No footnote section detected (no marker appears 2+ times)")
        return -1


class CustomPatternFootnoteStrategy:
    """Strategy with custom regex pattern for footnote detection."""

    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern)

    def is_footnote_line(self, line: str) -> bool:
        """Check if line matches custom footnote pattern."""
        return bool(self.pattern.match(line))


class DocumentRepository:
    """Handles file I/O operations for documents."""

    def __init__(self, encoding: str = 'utf-8'):
        self.encoding = encoding
        self._logger = logging.getLogger(f"{__name__}.DocumentRepository")

    def read(self, path: Path) -> str:
        """Read content from file."""
        self._logger.debug(f"Reading file: {path}")
        try:
            with open(path, 'r', encoding=self.encoding) as f:
                return f.read()
        except UnicodeDecodeError as e:
            self._logger.error(f"Unicode decode error for {path}: {e}")
            raise
        except OSError as e:
            self._logger.error(f"OS error reading {path}: {e}")
            raise

    def write(self, path: Path, content: str) -> None:
        """Write content to file."""
        self._logger.debug(f"Writing file: {path}")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w', encoding=self.encoding) as f:
                f.write(content)
        except OSError as e:
            self._logger.error(f"OS error writing {path}: {e}")
            raise

    def find_text_files(self, directory: Path, recursive: bool = False) -> list[Path]:
        """Find all text files in directory."""
        pattern = '**/*.txt' if recursive else '*.txt'
        files = list(directory.glob(pattern))
        self._logger.info(f"Found {len(files)} text files in {directory}")
        return files


class DocumentParser:
    """
    Class-based parser for Arabic book pages.

    Uses strategy pattern for footnote detection and repository pattern
    for file I/O operations.
    """

    def __init__(
            self,
            footnote_strategy: FootnoteStrategy | None = None,
            repository: DocumentRepository | None = None
    ):
        self.footnote_strategy = footnote_strategy or ArabicIndicFootnoteStrategy()
        self.repository = repository or DocumentRepository()
        self._logger = logging.getLogger(f"{__name__}.DocumentParser")

    def parse(self, input_path: Path | str) -> Document:
        """
        Parse a single document.

        Args:
            input_path: Path to the input text file

        Returns:
            Document object with body and footnotes
        """
        input_path = Path(input_path)
        self._logger.info(f"Parsing document: {input_path}")

        try:
            content = self.repository.read(input_path)
        except OSError as e:
            self._logger.error(f"Failed to read {input_path}: {e}")
            raise

        lines = content.split('\n')

        # Use the new footnote detection logic if strategy supports it
        if isinstance(self.footnote_strategy, ArabicIndicFootnoteStrategy):
            footnote_start_idx = self.footnote_strategy.find_footnote_start_index(lines)

            if footnote_start_idx >= 0:
                body_lines = lines[:footnote_start_idx]
                footnote_lines = lines[footnote_start_idx:]
            else:
                # No footnotes detected - all content is body
                body_lines = lines
                footnote_lines = []
        else:
            # Fallback to original line-by-line logic for other strategies
            body_lines: list[str] = []
            footnote_lines: list[str] = []
            in_footnotes = False

            for line in lines:
                if not in_footnotes and self.footnote_strategy.is_footnote_line(line):
                    in_footnotes = True
                    footnote_lines.append(line)
                elif in_footnotes:
                    footnote_lines.append(line)
                else:
                    body_lines.append(line)

        body = '\n'.join(body_lines).strip()
        footnotes = '\n'.join(footnote_lines).strip()

        self._logger.info(
            f"Parsed {input_path.name}: {len(body_lines)} body lines, "
            f"{len(footnote_lines)} footnote lines"
        )

        return Document(
            source_path=input_path,
            body=body,
            footnotes=footnotes,
            metadata={
                'total_lines': len(lines),
                'body_lines': len(body_lines),
                'footnote_lines': len(footnote_lines)
            }
        )

    def save(self, document: Document, output_dir: Path | str) -> Path:
        """
        Save parsed document as markdown.

        Args:
            document: Document to save
            output_dir: Directory to save the output

        Returns:
            Path to the saved file
        """
        output_dir = Path(output_dir)
        output_filename = f"{document.source_path.stem}_parsed.md"
        output_path = output_dir / output_filename

        markdown_content = document.to_markdown()
        self.repository.write(output_path, markdown_content)

        self._logger.info(f"Saved parsed document to: {output_path}")
        return output_path

    def process(
            self,
            input_path: Path | str,
            output_dir: Path | str | None = None
    ) -> ProcessingResult:
        """
        Parse and optionally save a document.

        Args:
            input_path: Path to input file
            output_dir: Optional output directory

        Returns:
            ProcessingResult with document and status
        """
        try:
            document = self.parse(input_path)

            output_path = None
            if output_dir:
                output_path = self.save(document, output_dir)

            return ProcessingResult(
                document=document,
                status=ProcessingStatus.SUCCESS,
                output_path=output_path
            )
        except Exception as e:
            self._logger.error(f"Failed to process {input_path}: {e}")
            return ProcessingResult(
                document=None,
                status=ProcessingStatus.FAILED,
                error=str(e)
            )


class BatchDocumentParser:
    """
    Batch processor for multiple documents with async support.

    Uses semaphore to limit concurrent operations and prevent
    resource exhaustion when processing large file batches.
    """

    def __init__(
            self,
            parser: DocumentParser | None = None,
            max_concurrent: int = 5
    ):
        self.parser = parser or DocumentParser()
        self.max_concurrent = max_concurrent
        self._semaphore: asyncio.Semaphore | None = None
        self._logger = logging.getLogger(f"{__name__}.BatchDocumentParser")

    async def _process_single_async(
            self,
            input_path: Path,
            output_dir: Path | None
    ) -> ProcessingResult:
        """Process a single file with semaphore control."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

        async with self._semaphore:
            self._logger.debug(f"Processing: {input_path}")
            # Run blocking I/O in executor to not block event loop
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.parser.process(input_path, output_dir)
            )
            return result

    async def process_directory_async(
            self,
            input_dir: Path | str,
            output_dir: Path | str | None = None,
            recursive: bool = False
    ) -> list[ProcessingResult]:
        """
        Process all text files in a directory asynchronously.

        Args:
            input_dir: Directory containing input files
            output_dir: Optional output directory
            recursive: Whether to search subdirectories

        Returns:
            List of ProcessingResult for each file
        """
        input_dir = Path(input_dir)
        if output_dir:
            output_dir = Path(output_dir)

        files = self.parser.repository.find_text_files(input_dir, recursive)

        if not files:
            self._logger.warning(f"No text files found in {input_dir}")
            return []

        self._logger.info(f"Processing {len(files)} files with max concurrency {self.max_concurrent}")

        tasks = [
            self._process_single_async(f, output_dir)
            for f in files
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to ProcessingResult
        processed_results: list[ProcessingResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(ProcessingResult(
                    document=None,
                    status=ProcessingStatus.FAILED,
                    error=str(result)
                ))
                self._logger.error(f"Exception processing {files[i]}: {result}")
            else:
                processed_results.append(result)

        # Log summary
        success_count = sum(1 for r in processed_results if r.status == ProcessingStatus.SUCCESS)
        failed_count = sum(1 for r in processed_results if r.status == ProcessingStatus.FAILED)
        self._logger.info(f"Batch complete: {success_count} succeeded, {failed_count} failed")

        return processed_results

    def process_directory(
            self,
            input_dir: Path | str,
            output_dir: Path | str | None = None,
            recursive: bool = False
    ) -> list[ProcessingResult]:
        """
        Process all text files in a directory (synchronous wrapper).

        Args:
            input_dir: Directory containing input files
            output_dir: Optional output directory
            recursive: Whether to search subdirectories

        Returns:
            List of ProcessingResult for each file
        """
        return asyncio.run(
            self.process_directory_async(input_dir, output_dir, recursive)
        )


class DocumentParserFactory:
    """Factory for creating DocumentParser instances with different configurations."""

    @staticmethod
    def create_default() -> DocumentParser:
        """Create parser with default Arabic-Indic footnote detection."""
        return DocumentParser()

    @staticmethod
    def create_with_custom_pattern(pattern: str) -> DocumentParser:
        """Create parser with custom footnote pattern."""
        return DocumentParser(
            footnote_strategy=CustomPatternFootnoteStrategy(pattern)
        )

    @staticmethod
    def create_batch_parser(max_concurrent: int = 5) -> BatchDocumentParser:
        """Create batch parser with specified concurrency limit."""
        return BatchDocumentParser(max_concurrent=max_concurrent)


if __name__ == '__main__':
    # Example usage demonstrating all features
    import sys

    # Ensure UTF-8 encoding for Windows console
    if sys.platform == 'win32':
        sys.stdout.reconfigure(encoding='utf-8')

    # Single file processing
    print("=== Single File Processing ===")
    parser_ = DocumentParserFactory.create_default()
    input_file_ = Path(__file__).parent / 'raw_content/book_page_pg-5.txt'
    output_dir_ = Path(__file__).parent / 'parsed_content'

    if input_file_.exists():
        result_ = parser_.process(input_file_, output_dir_)
        if result_.status == ProcessingStatus.SUCCESS:
            print(f"[OK] Parsed successfully: {result_.output_path}")
            if result_.document:
                print(f"  Body: {result_.document.word_count} words")
                print(f"  Has footnotes: {result_.document.has_footnotes}")
        else:
            print(f"[FAIL] Failed: {result_.error}")
    else:
        print(f"Input file not found: {input_file_}")

    # Batch processing
    print("\n=== Batch Processing ===")
    batch_parser = DocumentParserFactory.create_batch_parser(max_concurrent=4)
    input_dir_ = Path(__file__).parent / 'raw_content'

    if input_dir_.exists():
        results_ = batch_parser.process_directory(input_dir_, output_dir_, recursive=True)
        success = sum(1 for r in results_ if r.status == ProcessingStatus.SUCCESS)
        failed = sum(1 for r in results_ if r.status == ProcessingStatus.FAILED)
        print(f"Batch complete: {success} succeeded, {failed} failed")
    else:
        print(f"Input directory not found: {input_dir_}")