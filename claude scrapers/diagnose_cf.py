"""
Quick diagnostic — visits New World with Camoufox (patched Firefox) and prints
what response Cloudflare returns (status, headers, body snippet).
"""
import asyncio
from camoufox.async_api import AsyncCamoufox

URL = "https://www.newworld.co.nz/shop/category/fruit-and-vegetables"

async def main():
    async with AsyncCamoufox(headless=False, locale=["en-NZ", "en-GB"]) as browser:
        page = await browser.new_page()

        print(f"Navigating to {URL} ...")
        resp = await page.goto(URL, wait_until="load", timeout=30_000)

        print(f"\nHTTP status: {resp.status if resp else 'None'}")
        if resp:
            headers = await resp.all_headers()
            cf_headers = {k: v for k, v in headers.items() if "cf-" in k or k == "server"}
            print(f"CF headers:  {cf_headers}")

        title = await page.title()
        print(f"Page title immediately: {title!r}")

        import asyncio as _asyncio
        import random as _random

        # Wait for Turnstile iframe to appear
        print("Looking for Turnstile checkbox iframe...")
        try:
            await page.wait_for_selector(
                "iframe[src*='challenges.cloudflare.com']",
                timeout=8_000,
            )
            print("  Turnstile iframe found")
        except Exception:
            print("  No Turnstile iframe found — trying body check")

        # Get the Turnstile frame
        turnstile_frame = None
        for frame in page.frames:
            if "challenges.cloudflare.com" in frame.url:
                turnstile_frame = frame
                break

        if turnstile_frame:
            print(f"  Frame URL: {turnstile_frame.url[:80]}")
            try:
                # Wait for the checkbox to be visible inside the iframe
                checkbox = await turnstile_frame.wait_for_selector(
                    "input[type='checkbox']",
                    timeout=5_000,
                    state="visible",
                )
                box = await checkbox.bounding_box()
                if box:
                    # Human-like: move to a random point first, then drift to checkbox
                    start_x = _random.randint(200, 600)
                    start_y = _random.randint(200, 500)
                    await page.mouse.move(start_x, start_y)
                    await _asyncio.sleep(_random.uniform(0.3, 0.7))

                    # Get absolute position of checkbox (iframe offset + element offset)
                    iframe_el = page.locator("iframe[src*='challenges.cloudflare.com']")
                    iframe_box = await iframe_el.bounding_box()
                    abs_x = (iframe_box["x"] if iframe_box else 0) + box["x"] + box["width"] / 2
                    abs_y = (iframe_box["y"] if iframe_box else 0) + box["y"] + box["height"] / 2

                    # Move in steps toward the checkbox
                    steps = _random.randint(8, 15)
                    for i in range(steps):
                        t = (i + 1) / steps
                        mx = start_x + (abs_x - start_x) * t + _random.uniform(-3, 3)
                        my = start_y + (abs_y - start_y) * t + _random.uniform(-3, 3)
                        await page.mouse.move(mx, my)
                        await _asyncio.sleep(_random.uniform(0.02, 0.06))

                    await _asyncio.sleep(_random.uniform(0.1, 0.3))
                    print(f"  Clicking checkbox at ({abs_x:.0f}, {abs_y:.0f})...")
                    await page.mouse.click(abs_x, abs_y)
                else:
                    print("  Could not get checkbox bounding box — clicking directly")
                    await checkbox.click()
            except Exception as e:
                print(f"  Checkbox interaction failed: {e}")
        else:
            print("  No Turnstile frame found in page.frames")

        # Wait up to 15s for the challenge to resolve (title changes / redirect)
        print("Waiting up to 15s for Turnstile to resolve...")
        for i in range(15):
            await _asyncio.sleep(1)
            title = await page.title()
            if "just a moment" not in title.lower():
                print(f"  Resolved after {i+1}s!")
                break
            print(f"  {i+1}s — still on challenge page")

        title = await page.title()
        url_now = page.url
        print(f"\nFinal page title: {title!r}")
        print(f"Final URL:        {url_now!r}")

        body = await page.evaluate("document.body ? document.body.innerText.substring(0, 300) : ''")
        print(f"Body snippet: {body[:300]!r}")

asyncio.run(main())
