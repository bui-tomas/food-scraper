#  Food Price Scraper
**This repository is part of broader Food Tracker ecosystem**

A daily automated pipeline that scrapes food prices from major Slovak retailers and stores them in a PostgreSQL database for price tracking and analysis.

## Features
- **Automated Daily Scraping:** Runs automatically every day at 7:15 AM CET/CEST on a local machine
- **Parallel Processing:** Async scraping with retry logic for reliability
- **Data preprocessing:** Data is processed before being uploaded to DB
- **Database connection:** Uploads data to Supabase DB
- **Telegram Notifier:** Sends outcome message to Telegram

## Data Source
The scraper collects data from [cenyslovensko.sk](https://cenyslovensko.sk/), Slovakia's government-backed price comparison portal which was launched in June 2025. 

## Installation
1. **Clone the repository**
   ```bash
   git clone https://github.com/bui-tomas/food-scraper.git
   cd food-scraper
   ```

2. **Create virtual environment**
   ```bash
   python -m venv env
   source env/bin/activate  
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Configure environment**
   
   Create a `.env` file in the project root:
   ```env
   DATABASE_URL=postgresql://user:password@host:port/database
   ```