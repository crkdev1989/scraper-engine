from __future__ import annotations

import heapq
import logging
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from scraper_engine.core.models import CrawlConfig, CrawlResult, RunContext
from scraper_engine.crawl.fetcher import Fetcher
from scraper_engine.crawl.link_scoring import sort_links
from scraper_engine.crawl.url_utils import extract_links, normalize_url, same_domain
from scraper_engine.outputs.paths import raw_page_path
from scraper_engine.utils.logging_utils import log_event


class Crawler:
    def __init__(self, fetcher: Fetcher, logger=None) -> None:
        self.fetcher = fetcher
        self.logger = logger

    def crawl(
        self,
        seed_url: str,
        crawl_config: CrawlConfig,
        run_context: RunContext,
    ) -> CrawlResult:
        normalized_seed = normalize_url(seed_url)
        result = CrawlResult(seed_url=normalized_seed)
        queued_urls: set[str] = set()
        seen_urls: set[str] = set()
        future_to_url: dict[Future, tuple[str, int]] = {}
        priority_queue: list[tuple[int, int, str, int]] = []
        sequence = 0

        def push_url(url: str, depth: int, score: int) -> None:
            nonlocal sequence
            normalized = normalize_url(url)
            if normalized in queued_urls or normalized in seen_urls:
                return
            heapq.heappush(priority_queue, (-score, sequence, normalized, depth))
            queued_urls.add(normalized)
            sequence += 1
            log_event(
                self.logger,
                logging.INFO,
                "CRAWL QUEUE ADD",
                "Added URL to crawl queue.",
                url=normalized,
                depth=depth,
                score=score,
            )

        push_url(normalized_seed, depth=0, score=10_000)
        max_workers = max(1, crawl_config.concurrency)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while (priority_queue or future_to_url) and len(result.pages) < crawl_config.max_pages:
                while (
                    priority_queue
                    and len(future_to_url) < max_workers
                    and len(result.pages) + len(future_to_url) < crawl_config.max_pages
                ):
                    _, _, url, depth = heapq.heappop(priority_queue)
                    queued_urls.discard(url)
                    seen_urls.add(url)
                    future = executor.submit(self.fetcher.fetch, url)
                    future_to_url[future] = (url, depth)
                    log_event(
                        self.logger,
                        logging.INFO,
                        "FETCH PAGE",
                        "Submitting page fetch.",
                        url=url,
                        depth=depth,
                    )

                if not future_to_url:
                    break

                done, _ = wait(future_to_url.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    queued_url, depth = future_to_url.pop(future)
                    page = future.result()
                    page.depth = depth
                    result.pages.append(page)
                    seen_urls.add(page.url)

                    if page.html and run_context.raw_pages_dir:
                        output_path = raw_page_path(
                            run_context.raw_pages_dir,
                            page.url,
                            len(result.pages),
                        )
                        output_path.write_text(page.html, encoding="utf-8")
                        log_event(
                            self.logger,
                            logging.INFO,
                            "WRITE OUTPUTS",
                            "Stored raw HTML page.",
                            url=page.url,
                            path=output_path.name,
                        )

                    if page.error:
                        result.errors.append(f"{page.url}: {page.error}")
                        log_event(
                            self.logger,
                            logging.WARNING,
                            "FETCH PAGE",
                            "Page fetch failed.",
                            url=page.url,
                            error=page.error,
                        )
                        continue

                    discovered_links = extract_links(page.url, page.html)
                    if crawl_config.same_domain_only:
                        discovered_links = [
                            link
                            for link in discovered_links
                            if same_domain(normalized_seed, link.url)
                        ]
                    discovered_links = [
                        link
                        for link in discovered_links
                        if self._should_follow_link(link.url, link.anchor_text, crawl_config)
                    ]

                    ranked_links = sort_links(discovered_links, crawl_config.priority_keywords)
                    page.links = [link.url for link in ranked_links]
                    for link in ranked_links:
                        push_url(link.url, depth=depth + 1, score=link.score)

                    log_event(
                        self.logger,
                        logging.INFO,
                        "FETCH PAGE",
                        "Processed fetched page and ranked discovered links.",
                        url=page.url,
                        discovered_links=len(page.links),
                        queued_links=len(priority_queue),
                        depth=depth,
                    )

        result.queued_links = [entry[2] for entry in sorted(priority_queue)]
        result.notes.append(
            f"Crawled {len(result.pages)} page(s) for {normalized_seed} with max_pages={crawl_config.max_pages}."
        )
        return result

    def _should_follow_link(
        self,
        url: str,
        anchor_text: str,
        crawl_config: CrawlConfig,
    ) -> bool:
        haystacks = (url.lower(), anchor_text.lower())

        exclude_keywords = [keyword.lower() for keyword in crawl_config.exclude_url_keywords]
        if exclude_keywords and any(
            keyword in haystack
            for keyword in exclude_keywords
            for haystack in haystacks
        ):
            return False

        include_keywords = [keyword.lower() for keyword in crawl_config.include_url_keywords]
        if not include_keywords:
            return True
        return any(
            keyword in haystack
            for keyword in include_keywords
            for haystack in haystacks
        )
