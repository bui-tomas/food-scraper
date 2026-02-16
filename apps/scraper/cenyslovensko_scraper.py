import asyncio
from playwright.async_api import async_playwright, Page, Browser, Locator
from tqdm.asyncio import tqdm
import random
import re
import json
from datetime import datetime
from typing import Optional, Any
from collections import Counter
from .config import TEST_CATEGORIES, CATEGORIES

TIMEOUT_SELECTOR = 10000
SLEEP_MIN = 1.0
SLEEP_MAX = 1.5
RETRY_ATTEMPTS = 5
WORKERS_MAX = 10
WORKERS_MIN = 5

SELECTORS = {
    # Category page selectors
    'product_image': 'img[alt^="Obrázok produktu"]',  
    'pagination_button': 'button[aria-label^="Stránka"]',  
    
    # Product detail page selectors
    'retailer_button': 'button[data-orientation="vertical"]:has(img[alt])', 
    'accordion_container': 'div[role="region"][data-orientation="vertical"]',
    'product_name': 'div > h3',
    'product_details': 'dl', 
    
    # Price selectors
    'price_with_vat': 'div[aria-labelledby="header_price"] p strong',
    'price_without_vat': 'div[aria-labelledby="header_priceWithoutTax"] p',
    'unit_price': 'div[aria-labelledby="header_unitPrice"] p:not(.govuk-visually-hidden)',  
    'discount_info': 'div[aria-labelledby="header_discount"] p',
    'retailer_logo': 'img[alt]',
}

class FoodScraper():
    '''
    Food scraper for cenyslovensko.sk which scrapes prices, labels, sources etc.
    '''

    def __init__(self, cats: list[str], headless: bool):
        self.cats = cats
        self.headless = headless

    async def scrape_urls(self, page: Page, base_url: str) -> Optional[list[str]]:
        '''
        Returns all individual product URLs for given category.
        '''
        
        try:
            parts = base_url.rstrip('/').split('/')
            cat = parts[-1]

            product_urls = []

            # Find page count
            pagination_count = await page.locator(SELECTORS['pagination_button']).count()
            if pagination_count == 0:
                return None

            last_button = page.locator(SELECTORS['pagination_button']).nth(-1)
            aria_label = await last_button.get_attribute('aria-label')
                
            match = re.search(r'Stránka (\d+)', aria_label)
            if match:
                total_pages = int(match.group(1))
            else:
                return None
            
            # Collect product URLs from pages
            for page_num in range(1, total_pages + 1):
                if 'currentPage=' in base_url:
                    url = re.sub(r'currentPage=\d+', f'currentPage={page_num}', base_url)
                else:
                    separator = '&' if '?' in base_url else '?'
                    url = f'{base_url}{separator}currentPage={page_num}'
                
                await page.goto(url, wait_until='commit')
                await page.wait_for_selector(SELECTORS['product_image'], state='attached', timeout=10000)

                product_images = await page.locator(SELECTORS['product_image']).all()

                # Extract href from each product
                for img in product_images:
                    product_card = img.locator('xpath=..').first
                    link = product_card.locator('a[href^="/detail/"]').first
                    href = await link.get_attribute('href')
                                    
                    if href:
                        if href.startswith('http'):
                            product_urls.append([href, cat])
                        else:
                            base = 'https://cenyslovensko.sk'
                            product_urls.append([f'{base}{href}', cat])

            return product_urls
            
        except Exception as e:
            print(f'❌ Error scraping URLs from {base_url}: {e}')
            return None 
        
    async def extract_product_data(self, page: Page, product_url: list[str]) -> list[dict[str, Any]]:
        '''
        Extract detailed data from individual product page
        '''

        async def get_text(locator: Locator, selector: str) -> str:
            return (await locator.locator(selector).first.text_content()).strip()
        
        all_retailer_data = []

        # Find all retailer buttons (accordion items)
        retailer_buttons = await page.locator(SELECTORS['accordion_container']).first.locator(SELECTORS['retailer_button']).all()

        field_mapping = {
            'Veľkosť balenia': 'package_size',
            'DPH': 'vat_rate',
            'Krajina pôvodu': 'country_of_origin',
            'Výrobca': 'producer',
            'Distribútor': 'distributor'
        }

        for idx, button in enumerate(retailer_buttons):
            try:
                retailer_data = {}
                
                # Extract data from collapsed button (always visible)
                retailer_data['retailer'] = await button.locator(SELECTORS['retailer_logo']).first.get_attribute('alt')
                retailer_data['price_with_vat'] = await get_text(button, SELECTORS['price_with_vat'])
                retailer_data['price_without_vat'] = await get_text(button, SELECTORS['price_without_vat'])
                retailer_data['unit_price'] = await get_text(button, SELECTORS['unit_price'])
                retailer_data['discount_end_date'] = await get_text(button, SELECTORS['discount_info'])
                retailer_data['product_url'] = product_url[0]
                retailer_data['category'] = product_url[1]
                
                # Single retailer is already expanded by default
                if len(retailer_buttons) > 1:
                    await button.evaluate('element => element.click()')

                panel_id = await button.get_attribute('aria-controls')    
                panel = page.locator(f'#{panel_id}').first

                if idx == 0:
                    main_product_name = await get_text(panel, SELECTORS['product_name'])
                retailer_data['product_name'] = main_product_name

                # Get all dt elements (labels)
                dt_elements = await panel.locator(SELECTORS['product_details']).first.locator('dt').all()
                
                for dt_elem in dt_elements:
                    label = await get_text(dt_elem, 'strong')
                    
                    if label not in field_mapping:
                        continue
                    
                    # Extract value (with special handling for multi-paragraph fields)
                    parent_div = dt_elem.locator('..').first
                    if label in ['Krajina pôvodu', 'Výrobca', 'Distribútor']:
                        dd_elems = await parent_div.locator('dd p').all()
                        texts = [await elem.text_content() for elem in dd_elems]
                        value = '; '.join(texts)
                        
                        if label == 'Krajina pôvodu':
                            value = 'slovakia' if 'slovensko' in value.lower() else 'foreign'
                    else:
                        value = await get_text(parent_div, 'dd p')
                    
                    # Single assignment using the mapping
                    retailer_data[field_mapping[label]] = value

                retailer_data = self.clean_product_data(retailer_data)
                all_retailer_data.append(retailer_data)

            except Exception as e:
                print(f'Error extracting retailer {idx}: {e}')
                import traceback
                traceback.print_exc()
                continue
        
        return all_retailer_data 
    
    def clean_product_data(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        '''
        Clean and normalize scraped data for database insertion
        '''

        cleaned = raw_data.copy()
        
        for field in ['price_with_vat', 'price_without_vat', 'unit_price']:
            if field in cleaned and cleaned[field]:
                price_str = cleaned[field].replace('\xa0', '').replace('€', '').replace('(bez DPH)', '').strip()
                
                if field == 'unit_price' and '/' in price_str:
                    unit_part = price_str.split('/')[1].strip()
                    cleaned['unit'] = unit_part
                    price_str = price_str.split('/')[0].strip() 
                
                if '–' in price_str or '-' in price_str:
                    separator = '–' if '–' in price_str else '-'
                    parts = price_str.split(separator)
                    cleaned[f'{field}_min'] = float(parts[0].replace(',', '.').strip())
                    cleaned[f'{field}_max'] = float(parts[1].replace(',', '.').strip())
                    cleaned[field] = None  
                else:
                    price = price_str.replace(',', '.')
                    cleaned[field] = float(price) if price else None
        
        if 'vat_rate' in cleaned and cleaned['vat_rate']:
            cleaned['vat_rate'] = float(cleaned['vat_rate'].replace('%', '').strip())
        
        if 'discount_end_date' in cleaned and cleaned['discount_end_date']:
            if cleaned['discount_end_date'] == '– –':
                cleaned['discount_end_date'] = None
            else:
                match = re.search(r'(\d{2}\.\d{2}\.\d{4})', cleaned['discount_end_date'])
                if match:
                    date_str = match.group(1)
                    from datetime import datetime
                    cleaned['discount_end_date'] = datetime.strptime(date_str, '%d.%m.%Y').date()
                else:
                    cleaned['discount_end_date'] = None
        
        return cleaned
    
    async def scrape_product(
        self, 
        browser: Browser, 
        url: list[str], 
        semaphore: asyncio.Semaphore
    ) -> tuple[bool, Optional[list[dict[str, Any]]], list[str]]: 
        '''
        Scrape a single product with semaphore control.
        Returns tuple: (success: bool, data: list or None, url: tuple)
        '''

        async with semaphore:
            await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            page = await browser.new_page()
            try:
                await page.goto(url[0], wait_until='domcontentloaded')
                await page.wait_for_selector(SELECTORS['accordion_container'], state='attached', timeout=TIMEOUT_SELECTOR)
                product_data = await self.extract_product_data(page, url)
                return (True, product_data, url)
            except Exception as e:
                return (False, None, url)
            finally:
                await page.close()
    
    async def scrape_batch(
        self, 
        browser: Browser, 
        urls: list[list[str]], 
        semaphore: asyncio.Semaphore
    ) -> tuple[list[list[dict[str, Any]]], list[list[str]]]:  
        '''
        Scrape a batch of URLs and return successful/failed lists.
        
        Returns:
            tuple: (successful_products, failed_urls)
        '''

        tasks = [self.scrape_product(browser, url, semaphore) for url in urls]
        
        successful_products = []
        failed_urls = []
        
        for coro in tqdm.as_completed(tasks, total=len(urls), desc='Scraping', unit='product'):
            success, product_data, url = await coro
            if success and product_data:
                successful_products.append(product_data)
            else:
                failed_urls.append(url)
        
        return successful_products, failed_urls
    
    async def scrape_page(self, custom_urls=None) -> list[list[dict[str, Any]]]:
        '''
        Main async scraping orchestrator with retry logic
        '''

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--dns-prefetch-disable'
                ]
            )
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()

            print('🔍 Discovering pages...')
            if custom_urls is None:
                all_urls = []

                # Fetch all product urls for all cats
                for cat in self.cats:
                    print(f'Accessing {cat}...')
                    
                    page = await context.new_page()
                    
                    max_attempts = 3
                    for attempt in range(1, max_attempts + 1):
                        await page.goto(cat, wait_until='domcontentloaded', timeout=60000)
                        await page.wait_for_selector(SELECTORS['product_image'], state='attached', timeout=10000)
                        
                        urls = await self.scrape_urls(page, cat)
                        
                        if urls:
                            all_urls.extend(urls)
                            break
                        else:
                            if attempt < max_attempts:
                                await asyncio.sleep(3)
                    
                    await page.close()
                    
                    if not urls:
                        print(f'🛑 Stopping scraper - cannot continue without all categories')
                        await browser.close()
                        return []
            else:
                all_urls = custom_urls

            await page.close()

            url_counts = Counter([url[0] for url in all_urls])
            duplicates = {url: count for url, count in url_counts.items() if count > 1}
            if duplicates:
                print(f'⚠️  {len(duplicates)} products appear in multiple categories')
                seen = set()
                all_urls = [url for url in all_urls if url[0] not in seen and not seen.add(url[0])]
                print(f'✅ Deduplicated to {len(all_urls)} unique products')

            print(f'✅ Total product pages to scrape: {len(all_urls)}')

            # Retry config
            attempt = 1
            urls_to_scrape = all_urls
            all_products = []

            while urls_to_scrape and attempt <= RETRY_ATTEMPTS:
                print(f'\n🔄 Attempt {attempt}/{RETRY_ATTEMPTS} - Processing {len(urls_to_scrape)} URLs')
                
                concurrency = WORKERS_MAX if attempt == 1 else WORKERS_MIN
                semaphore = asyncio.Semaphore(concurrency)
                
                # Scrape batch
                successful, failed = await self.scrape_batch(browser, urls_to_scrape, semaphore)
                all_products.extend(successful)
                                
                # Check for failures
                if failed:
                    print(f'⚠️  Failed: {len(failed)} URLs')
                    
                    if attempt < RETRY_ATTEMPTS:
                        print(f'⏸️  Waiting 5 seconds before retry...')
                        await asyncio.sleep(5)
                        urls_to_scrape = failed
                        attempt += 1
                    else:
                        print(f'\n❌ {len(failed)} URLs failed after all attempts:')
                        for fail_url in failed[:5]:
                            print(f'{fail_url[0]}')
                        break
                else:
                    break

            if not self.headless:
                input('\nPress Enter to close browser...')
            
            await browser.close()

            # Save locally just in case
            # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # json_filename = f'scraped_data_{timestamp}.json'
            # with open(json_filename, 'w', encoding='utf-8') as f:
            #     json.dump(all_products, f, ensure_ascii=False, indent=2, default=str)
            # print(f'💾 Saved to {json_filename}')
            
            print(f'\n📊 Final Results:')
            print(f'Total products scraped: {len(all_products)}')
            print(f'Success rate: {len(all_products)}/{len(all_urls)} ({len(all_products)/len(all_urls)*100:.1f}%)')
            
            return all_products, len(all_products)/len(all_urls)
                        
if __name__ == '__main__':
    scraper = FoodScraper(TEST_CATEGORIES, True)
    all_products = asyncio.run(scraper.scrape_page())
