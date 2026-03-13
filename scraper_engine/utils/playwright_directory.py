from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from scraper_engine.core.models import CrawlPage
from scraper_engine.utils.logging_utils import log_event
from scraper_engine.utils.text_utils import dedupe_preserve_order


DEFAULT_NAVIGATION_TIMEOUT_MS = 30000
DEFAULT_INTERACTION_TIMEOUT_MS = 5000
DEFAULT_POST_CLICK_WAIT_MS = 1200
DEFAULT_POST_COOKIE_WAIT_MS = 500
DEFAULT_MAX_LOAD_MORE_CLICKS = 100


@dataclass
class PlaywrightDirectoryResult:
    directory_page: CrawlPage
    profile_urls: list[str]
    cookie_accepted: bool
    load_more_clicks: int
    stop_reason: str


class PlaywrightDirectoryClient:
    def __init__(
        self,
        settings: dict[str, Any] | None = None,
        user_agent: str | None = None,
        logger=None,
    ) -> None:
        self.settings = settings or {}
        self.user_agent = user_agent
        self.logger = logger
        self._sync_playwright = None
        self._timeout_error = None
        self._playwright = None
        self._browser = None
        self._context = None
        self._directory_page = None
        self._detail_page = None

    def __enter__(self) -> "PlaywrightDirectoryClient":
        try:
            from playwright.sync_api import TimeoutError, sync_playwright
        except ImportError as error:
            raise RuntimeError(
                "Playwright support requires the 'playwright' package. "
                "Install it with 'py -3 -m pip install playwright' and "
                "'py -3 -m playwright install chromium'."
            ) from error

        self._sync_playwright = sync_playwright
        self._timeout_error = TimeoutError
        self._playwright = sync_playwright().start()
        browser_name = str(self.settings.get("browser", "chromium"))
        browser_factory = getattr(self._playwright, browser_name, None)
        if browser_factory is None:
            self.close()
            raise ValueError(
                f"Unsupported Playwright browser '{browser_name}'."
            )

        context_options: dict[str, Any] = {}
        if self.user_agent:
            context_options["user_agent"] = self.user_agent

        viewport = self.settings.get("viewport")
        if isinstance(viewport, dict):
            width = viewport.get("width")
            height = viewport.get("height")
            if isinstance(width, int) and isinstance(height, int):
                context_options["viewport"] = {"width": width, "height": height}

        self._browser = browser_factory.launch(
            headless=bool(self.settings.get("headless", True))
        )
        self._context = self._browser.new_context(**context_options)
        self._directory_page = self._context.new_page()
        self._detail_page = self._context.new_page()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._detail_page is not None:
            self._detail_page.close()
            self._detail_page = None
        if self._directory_page is not None:
            self._directory_page.close()
            self._directory_page = None
        if self._context is not None:
            self._context.close()
            self._context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright is not None:
            self._playwright.stop()
            self._playwright = None

    def open_directory(self, start_url: str) -> PlaywrightDirectoryResult:
        self._require_started()
        directory_page = self._directory_page
        assert directory_page is not None

        try:
            response = directory_page.goto(
                start_url,
                wait_until=str(self.settings.get("directory_wait_until", "domcontentloaded")),
                timeout=self._navigation_timeout_ms(),
            )
            self._wait_for_optional_selector(
                directory_page,
                self.settings.get("directory_ready_selector"),
            )
            cookie_accepted = self._accept_cookies(directory_page)
            load_more_clicks, stop_reason = self._expand_load_more(directory_page)
            profile_urls = self._collect_profile_urls(directory_page)
            page = self._build_crawl_page(
                requested_url=start_url,
                page=directory_page,
                response=response,
            )
            return PlaywrightDirectoryResult(
                directory_page=page,
                profile_urls=profile_urls,
                cookie_accepted=cookie_accepted,
                load_more_clicks=load_more_clicks,
                stop_reason=stop_reason,
            )
        except Exception as error:
            return PlaywrightDirectoryResult(
                directory_page=CrawlPage(
                    requested_url=start_url,
                    url=start_url,
                    error=str(error),
                ),
                profile_urls=[],
                cookie_accepted=False,
                load_more_clicks=0,
                stop_reason="directory_render_failed",
            )

    def fetch_detail_page(self, url: str) -> CrawlPage:
        self._require_started()
        detail_page = self._detail_page
        assert detail_page is not None

        try:
            response = detail_page.goto(
                url,
                wait_until=str(self.settings.get("detail_wait_until", "domcontentloaded")),
                timeout=self._navigation_timeout_ms(),
            )
            self._wait_for_optional_selector(
                detail_page,
                self.settings.get("detail_wait_selector"),
            )
            return self._build_crawl_page(
                requested_url=url,
                page=detail_page,
                response=response,
            )
        except Exception as error:
            return CrawlPage(
                requested_url=url,
                url=url,
                error=str(error),
            )

    def _require_started(self) -> None:
        if self._context is None:
            raise RuntimeError(
                "PlaywrightDirectoryClient must be used as a context manager."
            )

    def _navigation_timeout_ms(self) -> int:
        return int(self.settings.get("navigation_timeout_ms", DEFAULT_NAVIGATION_TIMEOUT_MS))

    def _interaction_timeout_ms(self) -> int:
        return int(
            self.settings.get("interaction_timeout_ms", DEFAULT_INTERACTION_TIMEOUT_MS)
        )

    def _accept_cookies(self, page) -> bool:
        selector = self.settings.get("cookie_accept_selector")
        if not selector:
            return False

        try:
            locator = page.locator(str(selector)).first
            if locator.count() == 0:
                return False
            locator.click(timeout=self._interaction_timeout_ms())
            page.wait_for_timeout(
                int(self.settings.get("post_cookie_wait_ms", DEFAULT_POST_COOKIE_WAIT_MS))
            )
            self._log(
                logging.INFO,
                "PLAYWRIGHT",
                "Accepted cookie prompt.",
                selector=selector,
            )
            return True
        except self._timeout_error:
            self._log(
                logging.INFO,
                "PLAYWRIGHT",
                "Cookie prompt selector was not actionable.",
                selector=selector,
            )
            return False
        except Exception as error:
            self._log(
                logging.INFO,
                "PLAYWRIGHT",
                "Cookie prompt selector failed safely.",
                selector=selector,
                error=error,
            )
            return False

    def _expand_load_more(self, page) -> tuple[int, str]:
        selector = self.settings.get("load_more_selector")
        if not selector:
            return 0, "load_more_not_configured"

        max_clicks = int(
            self.settings.get("max_load_more_clicks", DEFAULT_MAX_LOAD_MORE_CLICKS)
        )
        post_click_wait_ms = int(
            self.settings.get("post_click_wait_ms", DEFAULT_POST_CLICK_WAIT_MS)
        )
        click_count = 0

        while click_count < max_clicks:
            try:
                locator = page.locator(str(selector)).first
                if locator.count() == 0:
                    return click_count, "load_more_not_found"

                before_count = self._count_profile_links(page)
                locator.click(timeout=self._interaction_timeout_ms())
                click_count += 1
                page.wait_for_timeout(post_click_wait_ms)
                self._wait_for_network_idle(page)
                after_count = self._count_profile_links(page)
                self._log(
                    logging.INFO,
                    "PLAYWRIGHT",
                    "Clicked load more control.",
                    selector=selector,
                    click_count=click_count,
                    cards_before=before_count,
                    cards_after=after_count,
                )
                if after_count <= before_count:
                    return click_count, "load_more_no_new_results"
            except self._timeout_error:
                return click_count, "load_more_timeout"
            except Exception as error:
                self._log(
                    logging.INFO,
                    "PLAYWRIGHT",
                    "Stopped load more expansion safely.",
                    selector=selector,
                    click_count=click_count,
                    error=error,
                )
                return click_count, "load_more_unavailable"

        return click_count, "max_load_more_clicks_reached"

    def _collect_profile_urls(self, page) -> list[str]:
        selector = self.settings.get("profile_link_selector")
        if not selector:
            raise ValueError(
                "Playwright renderer requires metadata.playwright.profile_link_selector."
            )

        urls = page.eval_on_selector_all(
            str(selector),
            """
            (elements) =>
                elements
                    .map((element) => {
                        const rawHref = element.getAttribute("href");
                        if (!rawHref) {
                            return null;
                        }
                        return new URL(rawHref, document.baseURI).href;
                    })
                    .filter(Boolean)
            """,
        )
        return dedupe_preserve_order([str(url) for url in urls if url])

    def _count_profile_links(self, page) -> int:
        selector = self.settings.get("profile_link_selector")
        if not selector:
            return 0
        return int(page.locator(str(selector)).count())

    def _wait_for_optional_selector(self, page, selector: Any) -> None:
        if not selector:
            return
        try:
            page.locator(str(selector)).first.wait_for(
                state="visible",
                timeout=self._interaction_timeout_ms(),
            )
        except Exception:
            self._log(
                logging.INFO,
                "PLAYWRIGHT",
                "Optional wait selector was not found before timeout.",
                selector=selector,
            )

    def _wait_for_network_idle(self, page) -> None:
        try:
            page.wait_for_load_state("networkidle", timeout=self._interaction_timeout_ms())
        except Exception:
            return

    def _build_crawl_page(self, requested_url: str, page, response) -> CrawlPage:
        final_url = page.url or requested_url
        return CrawlPage(
            requested_url=requested_url,
            url=final_url,
            status_code=response.status if response is not None else None,
            html=page.content(),
            redirected=final_url != requested_url,
        )

    def _log(self, level: int, event: str, message: str, **kwargs: Any) -> None:
        log_event(self.logger, level, event, message, **kwargs)
