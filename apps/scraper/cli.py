import click
import asyncio
from .config import CATEGORIES
from .cenyslovensko_scraper import FoodScraper
from .database import Database
from .notifier import Notifier

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
    
    notifier = Notifier()
    notifier.send_message('📤 <b>Notification</b>\n\nStarting cron job!')

    try:
        delimit('Scraping products', 1)
        
        # Scrape data
        scraper = FoodScraper(CATEGORIES, headless=True)
        all_products, success_rate = asyncio.run(scraper.scrape_page())
        
        delimit('Saving to DB', 2)

        # Save to database
        async def save_data():
            async with Database() as db:
                total_saved = await db.save_scraped_data(all_products)
                return total_saved
        
        total_saved = asyncio.run(save_data())
        click.echo(f'✅ Saved {total_saved} price records to database')

        if success_rate == 1.0:
            notifier.send_success(len(all_products))
        else:
            notifier.send_partial_success(len(all_products), success_rate)

    except Exception as e:
        click.echo(f'❌ Error: {e}')
        notifier.send_failure(str(e))
        raise

@cli.command()
def validate():
    print()

@cli.command()
def test_db():
    '''
    Test database connection
    '''

    try:
        async def test():
            async with Database() as db:
                return True
        
        asyncio.run(test())
        click.echo('✅ Database connection successful!')
    except Exception as e:
        click.echo(f'❌ Database connection failed: {e}')

@cli.command()
def test_telegram():
    '''
    Test Telegram notification
    '''

    notifier = Notifier()
    
    if not notifier.enabled:
        click.echo('❌ Telegram not configured. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env')
        return
    
    click.echo('📤 Sending test notification...')
    success = notifier.send_message('🧪 <b>Test notification</b>\n\nYour food scraper Telegram bot is working!')
    
    if success:
        click.echo('✅ Test notification sent! Check your Telegram.')
    else:
        click.echo('❌ Failed to send notification. Check your credentials.')

if __name__ == '__main__':
    cli()