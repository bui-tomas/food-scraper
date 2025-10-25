import click
import asyncio
from .config import CATEGORIES
from .cenyslovensko_scraper import FoodScraper
from .database import Database

def delimit(message, number):
    print(f'\n\n### {number}. {message}')
    print('-' * 100)

@click.group()
def cli():
    '''
    Ceny Slovensko Food Scraper CLI
    '''
    pass

@cli.command()
def scrape():
    '''
    Scrape prices from cenyslovensko.sk and save to database
    '''
    
    click.echo(delimit('Scraping products', 1))
    
    # Scrape data
    scraper = FoodScraper(CATEGORIES, headless=True)
    all_products = asyncio.run(scraper.scrape_page())
        
    # Save to database
    if all_products:
            async def save_data():
                async with Database() as db:
                    total_saved = await db.save_scraped_data(all_products)
                    return total_saved
            
            total_saved = asyncio.run(save_data())
            click.echo(f'✅ Saved {total_saved} price records to database')
    else:
        click.echo('⚠️  No products scraped')

@cli.command()
def test_db():
    '''
    Test database connection
    '''

    try:
        with Database() as db:
            click.echo('✅ Database connection successful!')
    except Exception as e:
        click.echo(f'❌ Database connection failed: {e}')

if __name__ == '__main__':
    cli()