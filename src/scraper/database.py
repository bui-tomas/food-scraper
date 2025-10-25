import os
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from datetime import date
from dotenv import load_dotenv

load_dotenv()

class Database:
    '''
    PostgreSQL database connection and operations
    '''

    def __init__(self):
        self.connection_string = os.getenv('DATABASE_URL')
        if not self.connection_string:
            raise ValueError('DATABASE_URL not found in .env file')
        self.conn = None

    async def __aenter__(self):
        '''
        Async context manager entry
        '''

        self.conn = await AsyncConnection.connect(
            self.connection_string,
            row_factory=dict_row
        )
        print('✅ Database connected')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        '''
        Async context manager exit
        '''

        if self.conn:
            await self.conn.close()
            print('Database connection closed')

    async def get_retailer_id(self, retailer_name):
        '''
        Get retailer ID by name (case-insensitive)
        '''

        retailer_map = {
            'lidl': 'Lidl',
            'kaufland': 'Kaufland',
            'tesco': 'Tesco',
            'billa': 'Billa',
            'fresh plus': 'Fresh Plus',
            'terno': 'Terno'
        }
        
        normalized_name = retailer_map.get(retailer_name.lower())
        if not normalized_name:
            raise ValueError(f'Retailer "{retailer_name}" not recognized')
        
        async with self.conn.cursor() as cur:
            await cur.execute(
                'SELECT id FROM retailers WHERE name = %s',
                (normalized_name,)
            )
            result = await cur.fetchone()
            if result:
                return result['id']
            else:
                raise ValueError(f'Retailer "{normalized_name}" not found in database')

    async def get_or_create_product(self, product_data):
        '''
        Get existing product or create new one. Returns product_id.
        '''

        product_url = product_data['product_url']
        
        async with self.conn.cursor() as cur:
            # Try to find existing product
            await cur.execute(
                'SELECT id FROM products WHERE product_url = %s',
                (product_url,)
            )
            result = await cur.fetchone()
            
            if result:
                # Update last_seen
                await cur.execute(
                    'UPDATE products SET last_seen = CURRENT_TIMESTAMP WHERE id = %s',
                    (result['id'],)
                )
                await self.conn.commit()
                return result['id']
            
            # Create new product
            await cur.execute(
                '''
                INSERT INTO products 
                (product_url, name, category, package_size, country_of_origin, 
                 producer, distributor)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                ''',
                (
                    product_url,
                    product_data.get('product_name'),
                    product_data.get('category'),
                    product_data.get('package_size'),
                    product_data.get('country_of_origin'),
                    product_data.get('producer'),
                    product_data.get('distributor')
                )
            )
            await self.conn.commit()
            new_id = (await cur.fetchone())['id']
            return new_id

    async def insert_price(self, product_id, retailer_id, price_data, scrape_date):
        '''Insert or update price record.'''
        async with self.conn.cursor() as cur:
            await cur.execute(
                '''
                INSERT INTO prices 
                (product_id, retailer_id, 
                 price_with_vat, price_without_vat, unit_price, unit,
                 price_with_vat_min, price_without_vat_min, unit_price_min,
                 price_with_vat_max, price_without_vat_max, unit_price_max,
                 vat_rate, discount_end_date, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id, retailer_id, date) 
                DO UPDATE SET
                    price_with_vat = EXCLUDED.price_with_vat,
                    price_without_vat = EXCLUDED.price_without_vat,
                    unit_price = EXCLUDED.unit_price,
                    unit = EXCLUDED.unit,
                    price_with_vat_min = EXCLUDED.price_with_vat_min,
                    price_without_vat_min = EXCLUDED.price_without_vat_min,
                    unit_price_min = EXCLUDED.unit_price_min,
                    price_with_vat_max = EXCLUDED.price_with_vat_max,
                    price_without_vat_max = EXCLUDED.price_without_vat_max,
                    unit_price_max = EXCLUDED.unit_price_max,
                    vat_rate = EXCLUDED.vat_rate,
                    discount_end_date = EXCLUDED.discount_end_date,
                    created_at = CURRENT_TIMESTAMP
                RETURNING id
                ''',
                (
                    product_id, retailer_id,
                    price_data.get('price_with_vat'),
                    price_data.get('price_without_vat'),
                    price_data.get('unit_price'),
                    price_data.get('unit'),
                    price_data.get('price_with_vat_min'),
                    price_data.get('price_without_vat_min'),
                    price_data.get('unit_price_min'),
                    price_data.get('price_with_vat_max'),
                    price_data.get('price_without_vat_max'),
                    price_data.get('unit_price_max'),
                    price_data.get('vat_rate'),
                    price_data.get('discount_end_date'),
                    scrape_date
                )
            )
            await self.conn.commit()
            return (await cur.fetchone())['id']

    async def save_product(self, retailer_data_list, category):
        '''
        Save one product with all its retailer prices.
        Returns number of prices saved.
        '''
        scrape_date = date.today()
        
        if not retailer_data_list:
            return 0
        
        # Get product info from first retailer
        first_retailer = retailer_data_list[0].copy()
        first_retailer['category'] = category
        
        # Get or create product
        product_id = await self.get_or_create_product(first_retailer)
        
        # Save price for each retailer
        saved_count = 0
        for retailer_data in retailer_data_list:
            try:
                retailer_id = await self.get_retailer_id(retailer_data['retailer'])
                await self.insert_price(product_id, retailer_id, retailer_data, scrape_date)
                saved_count += 1
            except Exception as e:
                print(f'❌ Error saving price for {retailer_data.get('retailer')}: {e}')
                continue
        
        return saved_count

    async def save_scraped_data(self, all_products):
        '''
        Save all scraped products in parallel with progress bar.
        
        Args:
            all_products: List of [retailer_data_list, ...] from scraper
        
        Returns:
            Total number of prices saved
        '''
        from tqdm.asyncio import tqdm
        
        print('💾 Saving to database...')
        
        # Create tasks for parallel saving
        coroutines = []
        for product_retailers in all_products:
            if product_retailers:
                category = product_retailers[0].get('category', 'unknown')
                coro = self.save_product(product_retailers, category)
                coroutines.append(coro)
        
        # Run all tasks in parallel with progress bar
        total_saved = 0
        for coro in tqdm.as_completed(coroutines, total=len(coroutines), desc='Saving', unit='product'):
            saved_count = await coro
            total_saved += saved_count
        
        return total_saved