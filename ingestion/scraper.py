"""
Turath.io Book Scraper

A class-based scraper for extracting book content, metadata, and related information
from turath.io with robust error handling and retry logic.
"""
from playwright.sync_api import sync_playwright, Page, Browser, Response, WebError, Playwright
from pathlib import Path
from typing import Optional, Dict, List, Any
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class TurathScraper:
    """
    A class-based scraper for turath.io books.

    Handles extraction of book metadata, content, table of contents,
    author information, and categories with robust error handling.
    """

    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds

    # Timeout configuration (adjusted for large books)
    PAGE_LOAD_TIMEOUT = 120000  # 2 minutes for page load
    ELEMENT_WAIT_TIMEOUT = 30000  # 30 seconds for element wait
    SCROLL_TIMEOUT = 600000  # 10 minutes for scrolling (large books)

    def __init__(self, headless: bool = False, base_output_dir: str = 'raw'):
        """
        Initialize the scraper.

        Args:
            headless: Run browser in headless mode
            base_output_dir: Base directory for storing scraped content
        """
        self.headless = headless
        self.base_output_dir = Path(__file__).parent.parent / 'data' / base_output_dir
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self._author_data: Dict[str, Any] = {}

    def _setup_book_directory(self, book_ref: str) -> Path:
        """
        Set up the landing zone directory structure for a book.

        Args:
            book_ref: Book reference number (e.g., "9472")

        Returns:
            Path to the book's directory
        """
        book_dir = self.base_output_dir / f"book_{book_ref}"
        book_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created book directory: {book_dir}")
        return book_dir

    def _retry_operation(self, operation, *args, **kwargs):
        """
        Execute an operation with retry logic.

        Args:
            operation: Callable to execute
            *args, **kwargs: Arguments to pass to the operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retries fail
        """
        last_exception = None
        for attempt in range(self.MAX_RETRIES):
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt + 1}/{self.MAX_RETRIES}")
                    time.sleep(self.RETRY_DELAY * attempt)  # Exponential backoff
                return operation(*args, **kwargs)

            except Exception as e:
                last_exception = e
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")

        logger.error(f"All {self.MAX_RETRIES} attempts failed")
        raise last_exception

    def start_browser(self):
        """Start the browser and create a new page."""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=self.headless)
            self.page = self.browser.new_page()
            logger.debug("Browser started")

    def close_browser(self):
        """Close the browser and clean up resources."""
        if self.browser:
            self.browser.close()
            self.browser = None
            self.page = None
            logger.debug("Browser closed")
        
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
            logger.debug("Playwright stopped")

    @staticmethod
    def _extract_title_and_author(page: Page) -> tuple[str, str]:
        """
        Extract book title and author from page.

        Args:
            page: Playwright page object

        Returns:
            Tuple of (title, author)
        """
        title_button = page.query_selector('h3.flex.text-\\[1\\.1rem\\].svelte-twub32 button')
        author_button = page.query_selector('h3.flex.text-\\[1\\.1rem\\].svelte-twub32 button:nth-of-type(2)')

        # Fallback: get all buttons and access by index
        if not title_button or not author_button:
            buttons = page.query_selector_all('h3.flex button')
            title_button = buttons[0] if len(buttons) > 0 else None
            author_button = buttons[1] if len(buttons) > 1 else None

        title = title_button.inner_text() if title_button else "Title not found"
        author = author_button.inner_text() if author_button else "Author not found"
        return title, author

    @staticmethod
    def _extract_category(page: Page) -> str:
        """
        Extract book category from page.

        Args:
            page: Playwright page object

        Returns:
            Category string
        """
        info_element = page.query_selector('.info.svelte-5q2e5v')
        if not info_element:
            return "Category not found"

        links = info_element.query_selector_all('a')
        if len(links) >= 2:
            return links[1].inner_text()
        if len(links) == 1:
            return links[0].inner_text()

        text = info_element.inner_text()
        parts = [p.strip() for p in text.split('·')]
        return parts[1] if len(parts) >= 2 else "Category not found"

    @staticmethod
    def _extract_size(page: Page) -> str:
        """
        Extract book size from page.

        Args:
            page: Playwright page object

        Returns:
            Size string
        """
        size_element = page.query_selector('.size.svelte-5q2e5v')
        return size_element.inner_text() if size_element else "Size not found"

    def extract_book_metadata(self, page: Page) -> Dict[str, str]:
        """
        Extract book metadata including title, author, category, and size.

        Args:
            page: Playwright page object

        Returns:
            Dictionary containing book metadata
        """

        def _extract():
            title, author = self._extract_title_and_author(page)
            category = self._extract_category(page)
            size = self._extract_size(page)

            return {
                "title": title,
                "author": author,
                "category": category,
                "size": size
            }

        return self._retry_operation(_extract)

    def extract_categories(self, page: Page) -> List[Dict[str, str]]:
        """
        Extract the list of categories and book counts.

        Args:
            page: Playwright page object

        Returns:
            List of category dictionaries
        """

        def _extract():
            categories = []
            category_items = page.query_selector_all('.category-item.svelte-1qwe70x')

            for item in category_items:
                name_el = item.query_selector('a span:nth-child(1)')
                count_el = item.query_selector('a .book-count.svelte-1qwe70x')

                if name_el and count_el:
                    categories.append({
                        "name": name_el.inner_text(),
                        "book_count": count_el.inner_text()
                    })

            return categories

        return self._retry_operation(_extract)

    def extract_toc(self, page: Page) -> List[Dict[str, str]]:
        """
        Extract the table of contents.

        Args:
            page: Playwright page object

        Returns:
            List of TOC items with heading location and text
        """

        def _extract():
            toc_items = []
            toc_buttons = page.query_selector_all('.toc-wrapper.svelte-twub32 ol.svelte-p5bu8 button')

            for button in toc_buttons:
                heading_loc_el = button.query_selector('.heading-loc.svelte-p5bu8')
                heading_loc = heading_loc_el.inner_text() if heading_loc_el else ""

                full_text = button.inner_text()
                heading_text = full_text.replace(heading_loc, '').strip() if heading_loc else full_text.strip()

                toc_items.append({
                    "heading_loc": heading_loc,
                    "heading_text": heading_text
                })

            return toc_items

        return self._retry_operation(_extract)

    def extract_author_panel_content(self, page: Page) -> Dict[str, Any]:
        """
        Extract author information by intercepting network requests.

        Args:
            page: Playwright page object

        Returns:
            Dictionary containing author information
        """
        self._author_data = {}

        def handle_response(response: Response):
            if "author" in response.url and response.status == 200:
                try:
                    self._author_data = response.json()
                    logger.debug("Author panel response captured")

                except WebError:
                    self._author_data = {"raw_content": response.text()}

        page.on("response", handle_response)

        def _extract():
            # Find and click author button
            buttons = page.query_selector_all('h3.flex.text-\\[1\\.1rem\\].svelte-twub32 button')

            author_button = None
            for idx, button in enumerate(buttons):
                if idx == 1:  # Author button is typically the second one
                    author_button = button
                    break

            if author_button:
                author_button.click()
                page.wait_for_timeout(3000)  # Wait for network request

            return self._author_data

        return self._retry_operation(_extract)

    def _wait_for_page_elements(self, page: Page) -> None:
        """
        Wait for essential page elements to load.

        Args:
            page: Playwright page object

        Raises:
            Exception: If required elements don't load within timeout
        """
        logger.info("Waiting for page metadata to load...")
        page.wait_for_selector('h3.flex button', timeout=self.ELEMENT_WAIT_TIMEOUT)
        page.wait_for_selector('.info', timeout=self.ELEMENT_WAIT_TIMEOUT, state='attached')
        page.wait_for_selector('.size', timeout=self.ELEMENT_WAIT_TIMEOUT, state='attached')
        page.wait_for_timeout(2000)

    @staticmethod
    def _save_json_data(data: Any, file_path: Path, description: str) -> None:
        """
        Save data to a JSON file.

        Args:
            data: Data to save
            file_path: Path to the output file
            description: Description of the data for logging

        Raises:
            Exception: If saving fails
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)  # type: ignore[arg-type]
        logger.info(f"Saved {description} to {file_path}")

    def _extract_and_save_metadata(self, page: Page, book_dir: Path) -> Dict[str, str]:
        """
        Extract and save book metadata.

        Args:
            page: Playwright page object
            book_dir: Directory to save metadata

        Returns:
            Dictionary containing book metadata
        """
        logger.info("Extracting book metadata...")
        metadata = self.extract_book_metadata(page)
        metadata_file = book_dir / "book_info.json"
        self._save_json_data(metadata, metadata_file, "metadata")
        return metadata

    def _extract_and_save_toc(self, page: Page, book_dir: Path) -> tuple[List[Dict[str, str]], Optional[str]]:
        """
        Extract and save table of contents.

        Args:
            page: Playwright page object
            book_dir: Directory to save TOC

        Returns:
            Tuple of (toc_items, error_message or None)
        """
        logger.info("Extracting table of contents...")
        try:
            toc = self.extract_toc(page)
            toc_file = book_dir / "toc.json"
            self._save_json_data(toc, toc_file, f"{len(toc)} TOC items")
            return toc, None

        except Exception as e:
            error_msg = f"TOC extraction failed: {str(e)}"
            logger.warning(error_msg)
            return [], error_msg

    def _extract_and_save_author_info(self, page: Page, book_dir: Path) -> tuple[
        Optional[Dict[str, Any]], Optional[str]]:
        """
        Extract and save author information.

        Args:
            page: Playwright page object
            book_dir: Directory to save author info

        Returns:
            Tuple of (author_info_dict or None, error_message or None)
        """
        logger.info("Extracting author information...")
        try:
            author_data = self.extract_author_panel_content(page)
            if author_data and "info" in author_data:
                inner_data = json.loads(author_data["info"])
                author_info = {
                    "name": inner_data.get("name", ""),
                    "death": inner_data.get("death", "")
                }
                author_file = book_dir / "author_info.json"
                self._save_json_data(inner_data, author_file, "author info")
                return author_info, None
            return None, None

        except Exception as e:
            error_msg = f"Author info extraction failed: {str(e)}"
            logger.warning(error_msg)
            return None, error_msg

    def _load_all_pages(self, page: Page, max_scrolls: int = 500) -> tuple[int, Optional[str]]:
        """
        Scroll through the viewport to load all pages.

        Uses a robust scrolling strategy with proper waits for lazy-loaded content.
        Stops when scroll height stabilizes (no new content loaded).

        Args:
            page: Playwright page object
            max_scrolls: Maximum scroll iterations

        Returns:
            Tuple of (scroll_count, error_message or None)
        """
        logger.info("Waiting for viewport to load...")
        page.wait_for_selector('div.viewport.svelte-12k9sog', timeout=self.SCROLL_TIMEOUT)

        viewport = page.query_selector('div.viewport.svelte-12k9sog')
        if not viewport:
            return 0, "Viewport not found"

        logger.info("Scrolling to load all pages...")
        scroll_count = 0
        prev_scroll_height = 0
        stable_count = 0
        min_stable_checks = 5  # Require 5 consecutive stable heights to confirm end

        while scroll_count < max_scrolls:
            # Scroll to bottom
            page.evaluate("(viewport) => viewport.scrollTo(0, viewport.scrollHeight)", viewport)

            # Wait for lazy-loaded content
            try:
                page.wait_for_load_state("networkidle", timeout=3000)
            except Exception:
                pass
            page.wait_for_timeout(800)

            # Get current scroll height
            current_scroll_height = page.evaluate("(viewport) => viewport.scrollHeight", viewport)

            # Check if height has stabilized
            if current_scroll_height == prev_scroll_height:
                stable_count += 1
                if stable_count >= min_stable_checks:
                    logger.info(f"Content fully loaded at height {current_scroll_height} after {scroll_count} scrolls")
                    break
            else:
                stable_count = 0
                prev_scroll_height = current_scroll_height

            scroll_count += 1

            if scroll_count % 20 == 0:
                logger.info(f"Scroll progress: {scroll_count} scrolls, height: {current_scroll_height}")

        if scroll_count >= max_scrolls:
            logger.warning(f"Reached max scrolls limit ({max_scrolls})")
            return scroll_count, f"Reached max scrolls limit ({max_scrolls})"

        return scroll_count, None

    @staticmethod
    def _extract_and_save_pages(page: Page, book_dir: Path, max_pages: Optional[int] = None) -> tuple[
        int, Optional[str]]:
        """
        Extract and save page content.

        Args:
            page: Playwright page object
            book_dir: Directory to save pages
            max_pages: Maximum pages to extract (None for all)

        Returns:
            Tuple of (pages_saved, error_message or None)
        """
        logger.info("Extracting page content...")
        pages = page.query_selector_all('div.page.flex.flex-col.justify-between.svelte-twub32')
        logger.info(f"Found {len(pages)} pages")

        pages_saved = 0
        for idx, page_div in enumerate(pages):
            if max_pages and idx >= max_pages:
                logger.info(f"Reached max pages limit ({max_pages})")
                break

            page_id = page_div.get_attribute('id')
            content = page_div.inner_text()

            page_file = book_dir / f"book_page_{page_id}.txt"
            with open(page_file, 'w', encoding='utf-8') as f:
                f.write(content)

            pages_saved += 1
            if pages_saved % 50 == 0:
                logger.debug(f"Saved {pages_saved} pages...")

        logger.info(f"Saved {pages_saved} pages to {book_dir}")
        return pages_saved, None

    def _save_extraction_stats(self, stats: Dict[str, Any], book_dir: Path) -> None:
        """
        Save extraction statistics to a JSON file.

        Args:
            stats: Extraction statistics dictionary
            book_dir: Directory to save stats
        """
        stats_file = book_dir / "extraction_stats.json"
        self._save_json_data(stats, stats_file, "extraction stats")

    def extract_book_data(
            self,
            url: str,
            book_ref: Optional[str] = None,
            max_pages: Optional[int] = None,
            max_scrolls: int = 500
    ) -> Dict[str, Any] | None:
        """
        Scrape complete book content including metadata, pages, TOC, and author info.

        This method orchestrates the extraction process by delegating to specialized
        helper methods for each extraction task, providing better modularity and
        error isolation.

        Args:
            url: Book URL to scrape
            book_ref: Book reference number (extracted from URL if not provided)
            max_pages: Maximum pages to extract (None for all)
            max_scrolls: Maximum scroll iterations for large books

        Returns:
            Dictionary containing extraction results and statistics

        Raises:
            Exception: If critical extraction steps fail
        """
        start_time = time.time()

        # Extract book reference from URL if not provided
        if book_ref is None:
            book_ref = url.split('/')[-1]

        # Set up directory structure
        book_dir = self._setup_book_directory(book_ref)

        stats: Dict[str, Any] = {
            "book_ref": book_ref,
            "url": url,
            "start_time": datetime.now().isoformat(),
            "pages_extracted": 0,
            "metadata": None,
            "toc_items": 0,
            "author_info": None,
            "errors": []
        }

        try:
            self.start_browser()
            page = self.page

            logger.info(f"Navigating to {url}")
            page.goto(url, timeout=self.PAGE_LOAD_TIMEOUT)

            # Wait for page elements to load
            self._wait_for_page_elements(page)

            # Extract and save metadata (critical - will raise on failure)
            metadata = self._extract_and_save_metadata(page, book_dir)
            stats["metadata"] = metadata

            # Extract and save TOC (non-critical - errors are logged)
            toc, toc_error = self._extract_and_save_toc(page, book_dir)
            stats["toc_items"] = len(toc)
            if toc_error:
                stats["errors"].append(toc_error)

            # Extract and save author info (non-critical - errors are logged)
            author_info, author_error = self._extract_and_save_author_info(page, book_dir)
            stats["author_info"] = author_info
            if author_error:
                stats["errors"].append(author_error)

            # Load all pages by scrolling (critical - will raise on failure)
            scroll_count, scroll_error = self._load_all_pages(page, max_scrolls)
            if scroll_error:
                stats["errors"].append(scroll_error)
            else:
                logger.info(f"Scrolling completed after {scroll_count} scrolls, waiting for final content to render...")
                # Final wait to ensure all lazy-loaded content is fully rendered
                page.wait_for_timeout(2000)

            # Extract and save page content (critical - will raise on failure)
            pages_saved, pages_error = self._extract_and_save_pages(page, book_dir, max_pages)
            stats["pages_extracted"] = pages_saved
            if pages_error:
                stats["errors"].append(pages_error)

        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            stats["errors"].append(str(e))
            raise

        finally:
            self.close_browser()
            stats["end_time"] = datetime.now().isoformat()
            stats["duration_seconds"] = time.time() - start_time

            # Save extraction stats
            self._save_extraction_stats(stats, book_dir)

        return stats

    def extract_book_content(
            self,
            url: str,
            book_ref: Optional[str] = None,
            max_pages: Optional[int] = None,
            max_scrolls: int = 200
    ) -> Dict[str, Any]:
        """
        Deprecated: Use extract_book_data instead.

        This method is kept for backward compatibility.
        """
        logger.warning("extract_book_content is deprecated, use extract_book_data instead")
        return self.extract_book_data(url, book_ref, max_pages, max_scrolls)

    def scrape_categories_page(self, url: str = "https://app.turath.io") -> Optional[List[Dict[str, str]]]:
        """
        Scrape categories from the landing page.

        Args:
            url: URL to scrape categories from

        Returns:
            List of category dictionaries, or None on error
        """
        try:
            self.start_browser()
            page = self.page

            logger.info(f"Navigating to {url}")
            page.goto(url, timeout=self.PAGE_LOAD_TIMEOUT)
            page.wait_for_timeout(3000)

            categories = self.extract_categories(page)
            logger.info(f"Extracted {len(categories)} categories")
            return categories

        except Exception as e:
            logger.error(f"Error scraping categories: {e}")
            return None

        finally:
            self.close_browser()


def main():
    """Main entry point for the scraper."""
    url = "https://app.turath.io/book/6388"

    scraper = TurathScraper(headless=False)

    try:
        stats = scraper.extract_book_data(
            url=url,
            max_pages=None,  # Extract all pages
            max_scrolls=200
        )

        logger.info("\n=== Extraction Summary ===")
        logger.info(f"Book Reference: {stats['book_ref']}")
        logger.info(f"Pages Extracted: {stats['pages_extracted']}")
        logger.info(f"TOC Items: {stats['toc_items']}")
        logger.info(f"Duration: {stats['duration_seconds']:.2f} seconds")

        if stats['errors']:
            logger.warning(f"Errors encountered: {len(stats['errors'])}")
            for error in stats['errors']:
                logger.warning(f"  - {error}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


if __name__ == '__main__':
    main()