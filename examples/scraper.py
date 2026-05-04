"""Web scraper example (mocked).

Demonstrates: chained handles, fan-out, caching, non-AI use case.

Run:
    cd examples && python scraper.py

Second run is instant (fully cached).
"""

from __future__ import annotations

import asyncio
import hashlib

from cairns import step, run, trace


@step(memo=True)  # cache fetched pages — the expensive leaf
async def fetch_page(url: str) -> str:
    """Mock HTTP fetch with simulated latency."""
    trace("fetching")
    await asyncio.sleep(0.1)
    h = hashlib.md5(url.encode()).hexdigest()[:6]
    return f"""<html>
<head><title>Page {h} - {url}</title></head>
<body>
  <h1>Content for {url}</h1>
  <p>Lorem ipsum dolor sit amet, price=$42.99, stock=150 units.</p>
  <a href="{url}/page2">Next</a>
  <a href="{url}/about">About</a>
</body>
</html>"""


@step
async def extract_links(html: str) -> list[str]:
    """Extract href links from HTML (simple regex mock)."""
    import re
    return re.findall(r'href="([^"]+)"', html)


@step
async def extract_product(html: str) -> dict[str, str]:
    """Extract product info from HTML."""
    import re

    title_match = re.search(r"<title>(.*?)</title>", html)
    price_match = re.search(r"price=\$?([\d.]+)", html)
    stock_match = re.search(r"stock=(\d+)", html)
    return {
        "title": title_match.group(1) if title_match else "Unknown",
        "price": price_match.group(1) if price_match else "0",
        "stock": stock_match.group(1) if stock_match else "0",
    }


@step
async def scrape_site(urls: list[str]) -> list[dict[str, str]]:
    """Scrape multiple URLs concurrently."""
    trace(f"starting scrape ({len(urls)} urls)")

    # Fan-out: fetch all pages concurrently
    pages = {url: fetch_page(url) for url in urls}

    # Chain: extract products from fetched pages (also concurrent)
    products_handles = {url: extract_product(page_handle) for url, page_handle in pages.items()}

    # Fan-in: collect results
    products: list[dict[str, str]] = []
    for url, handle in products_handles.items():
        product = await handle
        product["url"] = url
        products.append(product)
        trace(f"scraped: {product['title']}")

    trace(f"scrape complete ({len(products)} products)")
    return products



# Default entry point for `cairn run`
@step
async def main():
    urls = [
        "https://shop.example.com/product/1",
        "https://shop.example.com/product/2",
        "https://shop.example.com/product/3",
        "https://shop.example.com/product/4",
        "https://shop.example.com/product/5",
    ]
    return await scrape_site(urls)

if __name__ == "__main__":
    import time

    print("First run (fetches all pages)...")
    t0 = time.monotonic()
    products = run(main(), store_path=".cairns")
    t1 = time.monotonic()
    print(f"  {len(products)} products scraped in {t1 - t0:.2f}s\n")

    for p in products:
        print(f"  {p['title']} — ${p['price']} ({p['stock']} in stock)")

    print("\nSecond run (fully cached)...")
    t0 = time.monotonic()
    products2 = run(main(), store_path=".cairns")
    t1 = time.monotonic()
    print(f"  {len(products2)} products in {t1 - t0:.3f}s (cached)")
