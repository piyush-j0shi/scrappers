import asyncio
import json
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        captured = []

        async def intercept(route, request):
            if any(x in request.url for x in [
                "fulfil", "store", "pickup", "address",
                "changestore", "bookatime", "slot", "method"
            ]):
                body = None
                try:
                    body = request.post_data_json
                except Exception:
                    body = request.post_data
                entry = {
                    "method": request.method,
                    "url": request.url,
                    "body": body,
                    "headers": dict(request.headers),
                }
                captured.append(entry)
                print(f"\n>>> {request.method} {request.url}")
                if body:
                    print(f"    BODY: {body}")
            await route.continue_()

        page = await context.new_page()
        await page.route("**/*", intercept)
        await page.goto("https://www.woolworths.co.nz/bookatimeslot")

        print("\n--- Browser is open ---")
        print("1. Click Pickup")
        print("2. Search for a store (e.g. Invercargill)")
        print("3. Click the store to select it")
        print("4. Wait for the page to confirm the store change")
        print("\nClose the browser window when done.")

        # Wait for browser to close naturally
        await page.wait_for_event("close", timeout=300_000)

        out = "/tmp/ww_store_requests.json"
        with open(out, "w") as f:
            json.dump(captured, f, indent=2)
        print(f"\nCaptured {len(captured)} requests → {out}")

asyncio.run(main())
