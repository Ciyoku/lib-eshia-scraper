#!/usr/bin/env python3
"""Extract full book text from lib.eshia.ir reader pages."""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

PAGE_PATH_RE = re.compile(r"/(?P<book>\d+)/(?P<volume>\d+)/(?P<page>\d+)/?$")
WHITESPACE_RE = re.compile(r"\s+")

BLOCK_END_TAGS = {
    "address",
    "article",
    "aside",
    "blockquote",
    "dd",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "section",
    "table",
    "tr",
    "ul",
}

FORCED_BREAK_TAGS = {"br", "hr"}
MUTED_TAGS = {"script", "style", "noscript"}


@dataclass(frozen=True)
class PageRef:
    book_id: int
    volume: int
    page: int


def parse_page_ref(url: str) -> PageRef:
    match = PAGE_PATH_RE.search(urlparse(url).path)
    if not match:
        raise ValueError(
            "URL must end with /<book_id>/<volume>/<page>, got: " + url
        )
    return PageRef(
        book_id=int(match.group("book")),
        volume=int(match.group("volume")),
        page=int(match.group("page")),
    )


def canonical_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not path:
        path = "/"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def fetch_html(url: str, timeout: float, retries: int) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "ar,fa;q=0.9,en;q=0.5",
            },
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                try:
                    return raw.decode(charset)
                except LookupError:
                    return raw.decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError) as error:
            last_error = error
            if attempt < retries:
                time.sleep(0.8 * attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def discover_last_page_in_volume(
    current_url: str,
    current_ref: PageRef,
    hrefs: list[str],
) -> int | None:
    highest_page = current_ref.page
    found = False

    for href in hrefs:
        joined = canonical_url(urljoin(current_url, href))
        try:
            candidate_ref = parse_page_ref(joined)
        except ValueError:
            continue

        if candidate_ref.book_id != current_ref.book_id:
            continue
        if candidate_ref.volume != current_ref.volume:
            continue

        found = True
        if candidate_ref.page > highest_page:
            highest_page = candidate_ref.page

    if not found:
        return None
    return highest_page


def print_progress_bar(processed: int, total: int, current_ref: PageRef) -> None:
    width = 30
    safe_total = max(1, total)
    ratio = min(1.0, processed / safe_total)
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    percent = ratio * 100
    line = (
        f"\r[{bar}] {percent:6.2f}% {processed}/{safe_total} pages "
        f"| volume {current_ref.volume} page {current_ref.page}"
    )
    print(line, end="", file=sys.stderr, flush=True)


class EshiaPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []
        self.found_reader = False
        self._in_reader = False
        self._reader_td_depth = 0
        self._sticky_depth = 0
        self._muted_depth = 0
        self._footnote_depth = 0
        self._in_footnote_section = False
        self._footnote_separator_emitted = False
        self._pre_depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {name: value or "" for name, value in attrs}
        if tag == "a":
            href = attr_map.get("href")
            if href:
                self.hrefs.append(href)

        if not self._in_reader:
            if tag == "td" and self._has_class(attr_map.get("class", ""), "book-page-show"):
                self.found_reader = True
                self._in_reader = True
                self._reader_td_depth = 1
            return

        if tag == "td":
            self._reader_td_depth += 1

        if self._sticky_depth > 0:
            self._sticky_depth += 1
            return

        if tag == "div" and self._has_class(attr_map.get("class", ""), "sticky-menue"):
            self._sticky_depth = 1
            return

        if self._muted_depth > 0:
            self._muted_depth += 1
            return

        if tag in MUTED_TAGS:
            self._muted_depth = 1
            return

        if self._is_special_heading_tag(tag, attr_map):
            self._append_text("##")

        if self._footnote_depth > 0:
            self._footnote_depth += 1
        elif self._has_class(attr_map.get("class", ""), "footnote"):
            self._footnote_depth = 1
            self._in_footnote_section = True

        if tag == "hr":
            self._in_footnote_section = True

        if (
            tag == "a"
            and self._is_footnote_anchor(attr_map)
            and not self._footnote_separator_emitted
        ):
            self._append_footnote_separator()
            self._footnote_separator_emitted = True

        if tag == "pre":
            self._pre_depth += 1

        if tag in FORCED_BREAK_TAGS:
            self._append_newline(force=True)

    def handle_endtag(self, tag: str) -> None:
        if not self._in_reader:
            return

        if self._sticky_depth > 0:
            self._sticky_depth -= 1
        elif self._muted_depth > 0:
            self._muted_depth -= 1
        else:
            if self._footnote_depth > 0:
                self._footnote_depth -= 1

            if tag == "pre" and self._pre_depth > 0:
                self._pre_depth -= 1

            if tag in BLOCK_END_TAGS:
                self._append_newline(force=False)

        if tag == "td":
            self._reader_td_depth -= 1
            if self._reader_td_depth <= 0:
                self._in_reader = False
                self._reader_td_depth = 0
                self._footnote_depth = 0
                self._in_footnote_section = False
                self._footnote_separator_emitted = False

    def handle_data(self, data: str) -> None:
        if not self._in_reader or self._sticky_depth > 0 or self._muted_depth > 0:
            return
        self._append_text(data)

    @staticmethod
    def _has_class(class_value: str, class_name: str) -> bool:
        return class_name in class_value.split()

    def _is_special_heading_tag(self, tag: str, attr_map: dict[str, str]) -> bool:
        class_value = attr_map.get("class", "")
        if tag == "span" and self._has_class(class_value, "KalamateKhas"):
            return True
        if tag == "p" and self._has_class(class_value, "KalamateKhas"):
            return True
        if tag == "p" and self._has_class(class_value, "KalamateKhas2"):
            return True
        return False

    def _is_footnote_anchor(self, attr_map: dict[str, str]) -> bool:
        if not (self._footnote_depth > 0 or self._in_footnote_section):
            return False

        name = attr_map.get("name", "").lower()
        href = attr_map.get("href", "").lower()

        if "_ftn" in name:
            return True
        if "_ftn" in href or "#_ftn" in href:
            return True
        return False

    def _append_footnote_separator(self) -> None:
        if self._parts:
            self._parts[-1] = self._parts[-1].rstrip(" ")
            if self._parts[-1] == "":
                self._parts.pop()

        if self._parts and not self._parts[-1].endswith("\n"):
            self._parts.append("\n")

        self._parts.append("____________\n")

    def _append_text(self, text: str) -> None:
        if not text:
            return

        if self._pre_depth == 0:
            text = WHITESPACE_RE.sub(" ", text)

        if not text:
            return

        if self._parts:
            previous = self._parts[-1]
            if previous.endswith("\n"):
                text = text.lstrip(" ")
            elif previous.endswith(" "):
                text = text.lstrip(" ")
        else:
            text = text.lstrip(" ")

        if text:
            self._parts.append(text)

    def _append_newline(self, force: bool) -> None:
        if not self._parts:
            return

        self._parts[-1] = self._parts[-1].rstrip(" ")
        if self._parts[-1] == "":
            self._parts.pop()
            if not self._parts:
                return

        if force:
            self._parts.append("\n")
            return

        if not self._parts[-1].endswith("\n"):
            self._parts.append("\n")

    def reader_text(self) -> str:
        return "".join(self._parts).strip("\n")


def find_next_page_url(current_url: str, current_ref: PageRef, hrefs: list[str]) -> str | None:
    candidates: dict[tuple[int, int], str] = {}

    for href in hrefs:
        joined = canonical_url(urljoin(current_url, href))
        try:
            candidate_ref = parse_page_ref(joined)
        except ValueError:
            continue

        if candidate_ref.book_id != current_ref.book_id:
            continue
        if (candidate_ref.volume, candidate_ref.page) <= (
            current_ref.volume,
            current_ref.page,
        ):
            continue

        candidate_key = (candidate_ref.volume, candidate_ref.page)
        if candidate_key not in candidates:
            candidates[candidate_key] = joined

    if not candidates:
        return None

    return candidates[min(candidates.keys())]


def crawl_book(
    start_url: str,
    max_pages: int,
    delay_seconds: float,
    timeout: float,
    retries: int,
    quiet: bool,
) -> list[tuple[PageRef, str]]:
    current_url = canonical_url(start_url)
    start_ref = parse_page_ref(current_url)
    estimated_total_pages: int | None = None
    visited: set[PageRef] = set()
    pages: list[tuple[PageRef, str]] = []

    for _ in range(max_pages):
        current_ref = parse_page_ref(current_url)
        if current_ref in visited:
            break
        visited.add(current_ref)

        html = fetch_html(current_url, timeout=timeout, retries=retries)
        parser = EshiaPageParser()
        parser.feed(html)
        parser.close()

        if not parser.found_reader:
            raise RuntimeError(f"Reader element not found in: {current_url}")

        pages.append((current_ref, parser.reader_text()))
        discovered_last_page = discover_last_page_in_volume(
            current_url=current_url,
            current_ref=current_ref,
            hrefs=parser.hrefs,
        )
        if (
            discovered_last_page is not None
            and current_ref.volume == start_ref.volume
            and discovered_last_page >= start_ref.page
        ):
            estimated_total_pages = discovered_last_page - start_ref.page + 1

        if estimated_total_pages is not None and len(pages) > estimated_total_pages:
            estimated_total_pages = None

        if not quiet:
            display_total = max_pages
            if estimated_total_pages is not None:
                display_total = min(display_total, estimated_total_pages)
            print_progress_bar(
                processed=len(pages),
                total=display_total,
                current_ref=current_ref,
            )

        next_url = find_next_page_url(current_url, current_ref, parser.hrefs)
        if not next_url:
            break

        next_ref = parse_page_ref(next_url)
        if next_ref in visited:
            break

        current_url = next_url
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    if not quiet:
        print(file=sys.stderr)

    if len(pages) >= max_pages and not quiet:
        print(
            f"Stopped at --max-pages ({max_pages}).",
            file=sys.stderr,
        )

    return pages


def write_output(output_path: str, pages: list[tuple[PageRef, str]]) -> None:
    merged = "\nPAGE_SEPARATOR\n".join(text for _, text in pages)
    with open(output_path, "w", encoding="utf-8", newline="\n") as output_file:
        output_file.write(merged)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Extract full reader text from lib.eshia.ir book pages and follow "
            "internal page links automatically."
        )
    )
    parser.add_argument("start_url", help="Start URL, e.g. https://lib.eshia.ir/15050/1/0")
    parser.add_argument(
        "-o",
        "--output",
        default="book_text.txt",
        help="Output UTF-8 text file path (default: book_text.txt).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10000,
        help="Safety cap for total pages to fetch (default: 10000).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between page requests (default: 0).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout per request in seconds (default: 30).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retry count for request failures (default: 3).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable progress logs on stderr.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()

    if args.max_pages < 1:
        print("--max-pages must be >= 1", file=sys.stderr)
        return 2
    if args.retries < 1:
        print("--retries must be >= 1", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("--timeout must be > 0", file=sys.stderr)
        return 2
    if args.delay < 0:
        print("--delay must be >= 0", file=sys.stderr)
        return 2

    try:
        parse_page_ref(args.start_url)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2

    try:
        pages = crawl_book(
            start_url=args.start_url,
            max_pages=args.max_pages,
            delay_seconds=args.delay,
            timeout=args.timeout,
            retries=args.retries,
            quiet=args.quiet,
        )
        write_output(args.output, pages)
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Done. Pages extracted: {len(pages)}")
    print(f"Output file: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
