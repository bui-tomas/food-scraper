from playwright.sync_api import sync_playwright
from tqdm import tqdm
import re

class FoodScraper():
    '''
    Food scraper for cenyslovensko.sk which scrapes prices, labels, sources etc.
    '''

    def __init__(self, cats, headless):
        self.cats = cats
        self.headless = headless

    def get_product_urls(self, page, base_url):
        '''
        Returns all individual product URLs for given category
        '''

        product_urls = []

        # Find all pagination buttons
        pagination_buttons = page.locator('button[aria-label^="Stránka"]').all()

        if len(pagination_buttons) == 0:
            return None
        
        # Extract all page numbers from buttons
        page_nums = []
        for button in pagination_buttons:
            aria_label = button.get_attribute('aria-label')

            match = re.search(r'Stránka (\d+)', aria_label)
            if match:
                page_nums.append(int(match.group(1)))
        
        # Last page number
        total_pages = max(page_nums)
        
        # Collect product URLs from pages
        for page_num in range(1, total_pages + 1):
            if 'currentPage=' in base_url:
                url = re.sub(r'currentPage=\d+', f'currentPage={page_num}', base_url)
            else:
                separator = '&' if '?' in base_url else '?'
                url = f'{base_url}{separator}currentPage={page_num}'
            
            page.goto(url)
            page.wait_for_selector('img[alt^="Obrázok produktu"]', timeout=10000)
            products = page.locator('div[style*="flex-basis: calc(33.3333%"]').all()
            
            # Extract href from each product
            for product in products:
                # Find the <a> tag with <strong> inside it (product name link)
                link_element = product.locator('a strong').first
                parent_link = link_element.locator('..').first
                href = parent_link.get_attribute('href')
                
                if href:
                    # Construct full URL if needed
                    if href.startswith('http'):
                        product_urls.append(href)
                    else:
                        # Relative URL, construct full URL
                        base = 'https://cenyslovensko.sk'
                        product_urls.append(f'{base}{href}')

        return product_urls
    
    def extract_product_data(self, page, product_url):
        '''
        Extract detailed data from individual product page
        '''
        
        all_retailer_data = []

        # Find all retailer buttons (accordion items)
        retailer_buttons = page.locator('button.sc-dTvVRJ.jLGMQZ').all()
                
        for idx, button in enumerate(retailer_buttons):
            try:
                retailer_data = {}
                
                # Extract data from collapsed button (always visible)
                retailer_img = button.locator('img[alt]').first
                retailer_data['retailer'] = retailer_img.get_attribute('alt')
                
                price_strong = button.locator('p strong').first
                retailer_data['price_with_vat'] = price_strong.text_content().strip()

                price_without_vat = button.locator('p.sc-dntSTA.djIdwR.sc-cAnIvK.jUAbQe').first
                retailer_data['price_without_vat'] = price_without_vat.text_content().strip()
                
                unit_price = button.locator('p.sc-dntSTA.djIdwR.sc-fYRIQK.hhKUxF').first
                retailer_data['unit_price'] = unit_price.text_content().strip()

                discount = button.locator('div[aria-labelledby="header_discount"] p').first
                retailer_data['discount_validity'] = discount.text_content().strip()
                
                # Single retailer is already expanded by default
                if len(retailer_buttons) > 1:
                    button.evaluate('element => element.click()')
                    page.wait_for_timeout(1000)

                
                panel_id = button.get_attribute('aria-controls')    
                panel = page.locator(f'#{panel_id}').first

                product_name = panel.locator('h3.sc-kcLKEh.llLrQu').first
                retailer_data['product_name'] = product_name.text_content().strip()

                # Get all dt elements (labels)
                dl_container = panel.locator('dl.sc-eBIPcU.byIRsI').first
                dt_elements = dl_container.locator('dt').all()
                
                for dt_elem in dt_elements:
                    try:
                        # Get the label and description
                        label = dt_elem.locator('strong').first.text_content(timeout=2000).strip()
                        parent_div = dt_elem.locator('..').first
                        dd_elem = parent_div.locator('dd p').first
                        value = dd_elem.text_content(timeout=2000).strip()
                        
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
                        # Silently skip missing optional fields
                        continue

                
                retailer_data['product_url'] = product_url
                retailer_data = self.clean_product_data(retailer_data)
                all_retailer_data.append(retailer_data)
                
                # Close accordion (only if there are multiple)
                if len(retailer_buttons) > 1:
                    button.evaluate('element => element.click()')
                    page.wait_for_timeout(300)

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
                
                # For unit_price, extract unit first
                if field == 'unit_price' and '/' in price_str:
                    unit_part = price_str.split('/')[1].strip()
                    cleaned['unit'] = unit_part
                    price_str = price_str.split('/')[0].strip() 
                
                # Check for range: '0,45 – 0,50'
                if '–' in price_str or '-' in price_str:
                    separator = '–' if '–' in price_str else '-'
                    parts = price_str.split(separator)
                    cleaned[f'{field}_min'] = float(parts[0].replace(',', '.').strip())
                    cleaned[f'{field}_max'] = float(parts[1].replace(',', '.').strip())
                    cleaned[field] = None  
                else:
                    price = price_str.replace(',', '.')
                    cleaned[field] = float(price) if price else None
        
        # Clean VAT rate
        if 'vat_rate' in cleaned and cleaned['vat_rate']:
            cleaned['vat_rate'] = float(cleaned['vat_rate'].replace('%', '').strip())
        
        # Extract date from discount validity
        if 'discount_validity' in cleaned and cleaned['discount_validity']:
            if cleaned['discount_validity'] == '– –':
                cleaned['discount_end_date'] = None
            else:
                import re
                match = re.search(r'(\d{2}\.\d{2}\.\d{4})', cleaned['discount_validity'])
                if match:
                    date_str = match.group(1)
                    from datetime import datetime
                    cleaned['discount_end_date'] = datetime.strptime(date_str, '%d.%m.%Y').date()
                else:
                    cleaned['discount_end_date'] = None
        
        return cleaned
    
    def scrape_page(self):
        # Start Playwright
        with sync_playwright() as p:
            all_products = []

            # Launch browser and open a new page
            browser = p.chromium.launch(headless=self.headless)
            page = browser.new_page()

            print('🔍 Discovering pages...')
            all_urls = []

            # Iterate through all categories
            for cat in self.cats:
                print(f'Accessing {cat}...')
                page.goto(cat)

                # Wait for products to appear
                page.wait_for_selector('img[alt^="Obrázok produktu"]', timeout=10000)

                urls = self.get_product_urls(page, cat)

                all_urls.extend(urls)

            print(f'✅ Total product pages to scrape: {len(all_urls)}')
            print(f'{"-"*60}')

            with tqdm(total=len(all_urls), desc='Scraping', unit='page') as pbar:
                for url in all_urls:    
                    page.goto(url)
                    page.wait_for_selector('button.sc-dTvVRJ.jLGMQZ', timeout=10000) 

                    product_data = self.extract_product_data(page, url)
                    all_products.append(product_data)
                    
                    pbar.update(1)
            
            print(len(all_products))
            # Close browser
            if not self.headless:
                input('\nPress Enter to close browser...')
            browser.close()

            return all_products
                    
if __name__ == '__main__':
    scraper = FoodScraper(['https://cenyslovensko.sk/kategoria/1/chlieb-a-pecivo',
                        #    'https://cenyslovensko.sk/kategoria/2/mliecne-vyrobky-a-vajcia',
                        #    'https://cenyslovensko.sk/kategoria/3/maso-a-masove-vyrobky',
                        #    'https://cenyslovensko.sk/kategoria/4/zelenina-a-ovocie', 
                        #    'https://cenyslovensko.sk/kategoria/5/trvanlive-potraviny-a-jedla',
                        #    'https://cenyslovensko.sk/kategoria/6/specialne-potraviny'
                            ],
                          True)
    
    all_products = scraper.scrape_page()