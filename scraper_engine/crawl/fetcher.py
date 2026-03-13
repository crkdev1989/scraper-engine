from __future__ import annotations

import certifi
import logging
import requests
import urllib3

from scraper_engine.core.models import CrawlPage, RequestConfig
from scraper_engine.crawl.url_utils import normalize_url
from scraper_engine.utils.logging_utils import log_event
from scraper_engine.utils.retry import retry_call


class Fetcher:
    def __init__(self, config: RequestConfig, logger=None) -> None:
        self.config = config
        self.logger = logger

    def fetch(self, url: str) -> CrawlPage:
        if not self.config.verify_ssl:
            log_event(
                self.logger,
                logging.WARNING,
                "FETCH PAGE",
                "SSL verification is disabled by configuration.",
                url=url,
            )

        def _request(verify_ssl: bool) -> requests.Response:
            response = requests.get(
                url,
                timeout=self.config.timeout_seconds,
                headers={"User-Agent": self.config.user_agent},
                allow_redirects=True,
                verify=certifi.where() if verify_ssl else False,
            )
            response.raise_for_status()
            return response

        try:
            response = retry_call(
                func=lambda: _request(self.config.verify_ssl),
                attempts=self.config.retries,
                backoff_seconds=self.config.backoff_seconds,
                handled_exceptions=(requests.RequestException,),
                logger=self.logger,
            )
        except requests.exceptions.SSLError as error:
            if not self.config.allow_insecure_fallback:
                return CrawlPage(requested_url=url, url=url, error=str(error))
            log_event(
                self.logger,
                logging.WARNING,
                "FETCH PAGE",
                "SSL verification failed; retrying without certificate verification.",
                url=url,
                insecure_fallback=True,
            )
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            try:
                response = retry_call(
                    func=lambda: _request(False),
                    attempts=self.config.retries,
                    backoff_seconds=self.config.backoff_seconds,
                    handled_exceptions=(requests.RequestException,),
                    logger=self.logger,
                )
            except requests.RequestException as fallback_error:
                return CrawlPage(requested_url=url, url=url, error=str(fallback_error))
        except requests.RequestException as error:
            return CrawlPage(requested_url=url, url=url, error=str(error))

        try:
            content_type = response.headers.get("Content-Type", "").lower()
            if "html" not in content_type and "xml" not in content_type:
                return CrawlPage(
                    requested_url=url,
                    url=normalize_url(response.url),
                    status_code=response.status_code,
                    error=f"Unsupported content type: {content_type or 'unknown'}",
                )
            log_event(
                self.logger,
                logging.INFO,
                "FETCH PAGE",
                "Fetched page successfully.",
                requested_url=url,
                final_url=response.url,
                status_code=response.status_code,
            )
            return CrawlPage(
                requested_url=url,
                url=normalize_url(response.url),
                status_code=response.status_code,
                html=response.text,
                redirected=normalize_url(url) != normalize_url(response.url),
            )
        except requests.RequestException as error:
            return CrawlPage(requested_url=url, url=url, error=str(error))
