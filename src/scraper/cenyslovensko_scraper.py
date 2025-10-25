import asyncio
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm
import random
import re
from .config import CATEGORIES, TEST_CATEGORIES

class FoodScraper():
    '''
    Food scraper for cenyslovensko.sk which scrapes prices, labels, sources etc.
    '''

    def __init__(self, cats, headless):
        self.cats = cats
        self.headless = headless

    async def scrape_urls(self, page, base_url):
        '''
        Returns all individual product URLs for given category.
        '''
        
        try:
            parts = base_url.rstrip("/").split("/")
            cat = parts[-1]

            product_urls = []

            # Find page count
            pagination_count = await page.locator('button[aria-label^="Stránka"]').count()
            if pagination_count == 0:
                return None

            last_button = page.locator('button[aria-label^="Stránka"]').nth(-1)
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
                await page.wait_for_selector('div.sc-jvKoal.gTWuXg', timeout=10000)

                products = await page.locator('div.sc-jvKoal.gTWuXg').all()
                
                # Extract href from each product
                for product in products:
                    link_element = product.locator('a strong').first
                    parent_link = link_element.locator('..').first
                    href = await parent_link.get_attribute('href')
                                    
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
        
    async def extract_product_data(self, page, product_url):
        '''
        Extract detailed data from individual product page
        '''

        async def get_text(locator, selector):
            return (await locator.locator(selector).first.text_content()).strip()
        
        all_retailer_data = []

        # Find all retailer buttons (accordion items)
        retailer_buttons = await page.locator('button.sc-dTvVRJ.jLGMQZ').all()
                
        for idx, button in enumerate(retailer_buttons):
            try:
                retailer_data = {}
                
                # Extract data from collapsed button (always visible)
                retailer_data['retailer'] = await button.locator('img[alt]').first.get_attribute('alt')
                retailer_data['price_with_vat'] = await get_text(button, 'p strong')
                retailer_data['price_without_vat'] = await get_text(button, 'p.sc-dntSTA.djIdwR.sc-cAnIvK.jUAbQe')
                retailer_data['unit_price'] = await get_text(button, 'p.sc-dntSTA.djIdwR.sc-fYRIQK.hhKUxF')
                retailer_data['discount_end_date'] = await get_text(button, 'div[aria-labelledby="header_discount"] p')
                
                # Single retailer is already expanded by default
                if len(retailer_buttons) > 1:
                    await button.evaluate('element => element.click()')
                    await page.wait_for_timeout(200)

                panel_id = await button.get_attribute('aria-controls')    
                panel = page.locator(f'#{panel_id}').first

                if idx == 0:
                    main_product_name = await get_text(panel, 'h3.sc-kcLKEh.llLrQu')

                retailer_data['product_name'] = main_product_name

                # Get all dt elements (labels)
                dl_container = panel.locator('dl.sc-eBIPcU.byIRsI').first
                dt_elements = await dl_container.locator('dt').all()
                
                for dt_elem in dt_elements:
                    try:
                        label = await get_text(dt_elem, 'strong')
                        parent_div = dt_elem.locator('..').first
                        if label in ['Krajina pôvodu', 'Výrobca', 'Distribútor']:
                            dd_elems = await parent_div.locator('dd p').all()
                            texts = [await elem.text_content() for elem in dd_elems]
                            value = '; '.join(texts)
                        else:
                            dd_elem = await parent_div.locator('dd p').first.text_content()
                            value = (await dd_elem.text_content(timeout=2000)).strip()
                        
                        if label == 'Veľkosť balenia':
                            retailer_data['package_size'] = value
                        elif label == 'DPH':
                            retailer_data['vat_rate'] = value
                        elif label == 'Krajina pôvodu':
                            retailer_data['country_of_origin'] = value
                        elif label == 'Výrobca':
                            retailer_data['producer'] = value
                        elif label == 'Distribútor':
                            retailer_data['distributor'] = value
                        elif label == 'Odkaz na stránku predajcu':
                            retailer_data['retailer_link'] = value
                    except:
                        continue

                retailer_data['product_url'] = product_url[0]
                retailer_data['category'] = product_url[1]
                retailer_data = self.clean_product_data(retailer_data)
                all_retailer_data.append(retailer_data)

            except Exception as e:
                print(f'Error extracting retailer {idx}: {e}')
                import traceback
                traceback.print_exc()
                continue
        
        return all_retailer_data
    
    def clean_product_data(self, raw_data):
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
    
    async def scrape_product(self, browser, url, semaphore):
        '''
        Scrape a single product with semaphore control.
        Returns tuple: (success: bool, data: list or None, url: tuple)
        '''

        async with semaphore:
            await asyncio.sleep(random.uniform(1.0, 1.5))
            page = await browser.new_page()
            try:
                await page.goto(url[0], wait_until='domcontentloaded')
                await page.wait_for_selector('button.sc-dTvVRJ.jLGMQZ', state='attached', timeout=15000)
                product_data = await self.extract_product_data(page, url)
                return (True, product_data, url)
            except Exception as e:
                return (False, None, url)
            finally:
                await page.close()
    
    async def scrape_batch(self, browser, urls, semaphore):
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
    
    async def scrape_page(self):
        '''
        Main async scraping orchestrator with retry logic
        '''

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            page = await browser.new_page()

            print('🔍 Discovering pages...')
            all_urls = []

            # Fetch all product urls for all cats
            for cat in self.cats:
                print(f'Accessing {cat}...')
                
                max_attempts = 3
                urls = None
                
                for attempt in range(1, max_attempts + 1):
                    await page.goto(cat, wait_until='domcontentloaded')
                    await page.wait_for_selector('img[alt^="Obrázok produktu"]', state='attached', timeout=10000)
                    
                    urls = await self.scrape_urls(page, cat)
                    
                    if urls:
                        all_urls.extend(urls)
                        break
                    else:
                        if attempt < max_attempts:
                            await asyncio.sleep(3)
                
                    if not urls:
                        print(f'🛑 Stopping scraper - cannot continue without all categories')
                        await page.close()
                        await browser.close()
                        return []

            await page.close()

            print(f'✅ Total product pages to scrape: {len(all_urls)}')

            # Retry config
            max_attempts = 3
            attempt = 1
            urls_to_scrape = all_urls
            all_products = []

            while urls_to_scrape and attempt <= max_attempts:
                print(f'\n🔄 Attempt {attempt}/{max_attempts} - Processing {len(urls_to_scrape)} URLs')
                
                concurrency = 10 if attempt == 1 else 5
                semaphore = asyncio.Semaphore(concurrency)
                
                # Scrape batch
                successful, failed = await self.scrape_batch(browser, urls_to_scrape, semaphore)
                all_products.extend(successful)
                                
                # Check for failures
                if failed:
                    print(f'⚠️  Failed: {len(failed)} URLs')
                    
                    if attempt < max_attempts:
                        print(f'⏸️  Waiting 5 seconds before retry...')
                        await asyncio.sleep(5)
                        urls_to_scrape = failed
                        attempt += 1
                    else:
                        print(f'\n❌ {len(failed)} URLs failed after all attempts:')
                        for fail_url in failed[:5]:
                            print(f'   - {fail_url[0]}')
                        if len(failed) > 5:
                            print(f'   ... and {len(failed) - 5} more')
                        break
                else:
                    break

            if not self.headless:
                input('\nPress Enter to close browser...')
            
            await browser.close()
            
            print(f'\n📊 Final Results:')
            print(f'   Total products scraped: {len(all_products)}')
            print(f'   Success rate: {len(all_products)}/{len(all_urls)} ({len(all_products)/len(all_urls)*100:.1f}%)')
            
            return all_products
                        
if __name__ == '__main__':
    scraper = FoodScraper(TEST_CATEGORIES, True)
    all_products = asyncio.run(scraper.scrape_page())
