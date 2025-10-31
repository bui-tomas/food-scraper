import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class Notifier:
    '''
    Send notifications to Telegram bot
    '''
    
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.bot_token or not self.chat_id:
            print('⚠️  Telegram credentials not found in .env - notifications disabled')
            self.enabled = False
        else:
            self.enabled = True
    
    def send_message(self, message, parse_mode='HTML'):
        '''
        Send a message to Telegram
        '''

        if not self.enabled:
            return False
        
        url = f'https://api.telegram.org/bot{self.bot_token}/sendMessage'
        
        payload = {
            'chat_id': self.chat_id,
            'text': message,
            'parse_mode': parse_mode
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f'❌ Failed to send Telegram notification: {e}')
            return False
    
    def send_success(self, products_count):
        '''
        Send success notification
        '''

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        message = (
            f'✅ <b>Food Scraper Success</b>\n'
            f'📅 Date: {timestamp}\n'
            f'📦 Products: {products_count}'
        )
        
        return self.send_message(message)

    def send_failure(self, error_message):
        '''
        Send failure notification
        '''

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        error_message = error_message.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        
        message = (
            f'❌ <b>Food Scraper Failed</b>\n'
            f'📅 Date: {timestamp}\n'
            f'⚠️ Error: <code>{error_message}</code>'
        )
        
        return self.send_message(message)
    
    def send_partial_success(self, products_count, success_rate):
        '''
        Send partial success notification (some failures but completed)
        '''
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        message = (
            f'⚠️ <b>Food Scraper Completed with Warnings</b>\n'
            f'📅 Date: {timestamp}\n'
            f'📦 Products: {products_count}\n'
            f'✓ Success rate: {success_rate:.1%}'
        )
        
        return self.send_message(message)