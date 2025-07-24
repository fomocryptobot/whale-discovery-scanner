#!/usr/bin/env python3
"""
FCB Whale Discovery Scanner v5.0 - CRON OPTIMIZED
Converted from service to cron job - runs immediately and exits cleanly
"""

import requests
import time
import json
import os
from datetime import datetime, timedelta
import logging
import sys

try:
    import psycopg
    from psycopg import IntegrityError, DataError
except ImportError:
    print("❌ Installing psycopg...", flush=True)
    os.system("pip install psycopg[binary]")
    import psycopg
    from psycopg import IntegrityError, DataError

# Configuration - ALL KEYS FROM ENVIRONMENT VARIABLES
DB_URL = os.getenv('TRINITY_DATABASE_URL')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')

# Validate required environment variables
if not DB_URL:
    raise ValueError("❌ TRINITY_DATABASE_URL environment variable is required")
if not ETHERSCAN_API_KEY:
    raise ValueError("❌ ETHERSCAN_API_KEY environment variable is required") 
if not COINGECKO_API_KEY:
    raise ValueError("❌ COINGECKO_API_KEY environment variable is required")

WHALE_THRESHOLD_USD = 1000
ETHERSCAN_DELAY = 1.0
COINGECKO_DELAY = 0.15
MAX_USD_AMOUNT = 100_000_000
SCANNER_VERSION = "whale_discovery_v5.0_cron"

# CoinGecko Pro API
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

# CRITICAL: Force unbuffered output for Render Cron Jobs
os.environ['PYTHONUNBUFFERED'] = '1'
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Enhanced logging for Render Cron Jobs
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)

# Test log - this should appear in Render logs
logger.info("🚀 WHALE SCANNER CRON JOB STARTING")
logger.info(f"⏰ Execution time: {datetime.utcnow()}")

# High-value whale intelligence: Stablecoins + Blue Chips
TOP_TOKENS = {
    'USDC': {'address': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'decimals': 6, 'coingecko_id': 'usd-coin'},
    'USDT': {'address': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'decimals': 6, 'coingecko_id': 'tether'},
    'WETH': {'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'decimals': 18, 'coingecko_id': 'weth'},
    'WBTC': {'address': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'decimals': 8, 'coingecko_id': 'wrapped-bitcoin'},
    'UNI': {'address': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18, 'coingecko_id': 'uniswap'},
}

class EtherscanAPI:
    """Etherscan API with robust error handling"""
    
    def __init__(self, api_key, delay=ETHERSCAN_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = "https://api.etherscan.io/api"
        self.session = requests.Session()
    
    def get_latest_block(self):
        """Get latest block with fallbacks"""
        try:
            # Method 1: Direct API call
            params = {
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': self.api_key
            }
            
            time.sleep(self.delay)
            response = self.session.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'result' in data:
                    try:
                        block_num = int(data['result'], 16)
                        logger.info(f"✅ Latest block: {block_num:,}")
                        sys.stdout.flush()
                        return block_num
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            logger.warning(f"Block number lookup failed: {e}")
        
        # Fallback: estimate current block
        baseline_timestamp = 1704067200  # Jan 1, 2025
        baseline_block = 22000000
        current_timestamp = int(time.time())
        seconds_elapsed = current_timestamp - baseline_timestamp
        estimated_block = baseline_block + (seconds_elapsed // 12)
        
        logger.warning(f"⚠️ Using estimated block: {estimated_block:,}")
        return min(estimated_block, 23500000)  # Cap at reasonable max
    
    def get_token_transfers(self, contract_address, start_block, end_block):
        """Get token transfers with robust error handling"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': contract_address,
            'startblock': start_block,
            'endblock': end_block,
            'page': 1,
            'offset': 500,  # Reduced to avoid timeouts
            'sort': 'desc',
            'apikey': self.api_key
        }
        
        for attempt in range(3):
            try:
                time.sleep(self.delay)
                response = self.session.get(self.base_url, params=params, timeout=45)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == '1':
                        result = data.get('result', [])
                        if isinstance(result, list):
                            return result
                        else:
                            logger.warning(f"Unexpected result type: {type(result)}")
                            return []
                    elif data.get('message') == 'No transactions found':
                        return []
                    else:
                        logger.warning(f"Etherscan API: {data.get('message', 'Unknown error')}")
                        return []
                        
                else:
                    logger.warning(f"HTTP {response.status_code} - attempt {attempt + 1}")
                    
            except Exception as e:
                logger.warning(f"Transfer request failed (attempt {attempt + 1}): {e}")
            
            # Backoff before retry
            if attempt < 2:
                time.sleep(self.delay * 2)
        
        return []

class CoinGeckoProAPI:
    """CoinGecko Pro API with proper rate limiting"""
    
    def __init__(self, api_key, delay=COINGECKO_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = COINGECKO_PRO_BASE_URL
        self.headers = {'x-cg-pro-api-key': self.api_key}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_multiple_prices(self, coingecko_ids):
        """Get token prices with error handling"""
        try:
            if not coingecko_ids:
                return {}
            
            ids_string = ','.join(coingecko_ids)
            params = {
                'ids': ids_string,
                'vs_currencies': 'usd'
            }
            
            time.sleep(self.delay)
            response = self.session.get(f"{self.base_url}/simple/price", params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                prices = {}
                
                for coin_id, price_data in data.items():
                    price = price_data.get('usd', 0)
                    if price > 0:
                        prices[coin_id] = price
                
                logger.info(f"💰 Retrieved prices for {len(prices)} tokens")
                return prices
            else:
                logger.warning(f"CoinGecko error: HTTP {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Price lookup failed: {e}")
            return {}

class WhaleScanner:
    """Main whale scanner optimized for cron execution"""
    
    def __init__(self):
        self.etherscan = EtherscanAPI(ETHERSCAN_API_KEY)
        self.coingecko = CoinGeckoProAPI(COINGECKO_API_KEY)
        self.db_connection = None
    
    def connect_database(self):
        """Connect to database with autocommit disabled"""
        try:
            self.db_connection = psycopg.connect(DB_URL)
            self.db_connection.autocommit = False  # Enable transaction control
            logger.info("✅ Database connected")
            sys.stdout.flush()
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            sys.stderr.flush()
            return False
    
    def validate_transaction_data(self, tx):
        """Validate transaction data before database insert"""
        required_fields = ['symbol', 'transaction_hash', 'whale_address', 'amount_usd']
        
        try:
            # Check required fields
            for field in required_fields:
                if field not in tx or tx[field] is None or tx[field] == '':
                    return False
            
            # Validate transaction hash format
            if not isinstance(tx['transaction_hash'], str) or not tx['transaction_hash'].startswith('0x'):
                return False
            
            # Validate whale address format  
            if not isinstance(tx['whale_address'], str) or len(tx['whale_address']) != 42:
                return False
            
            # Validate USD amount range
            usd_amount = float(tx['amount_usd'])
            if usd_amount < WHALE_THRESHOLD_USD or usd_amount > MAX_USD_AMOUNT:
                return False
            
            return True
            
        except (ValueError, TypeError, KeyError):
            return False
    
    def save_transactions(self, transactions):
        """Save transactions with proper error handling"""
        if not transactions or not self.db_connection:
            return 0
        
        saved_count = 0
        
        for tx in transactions:
            # Validate each transaction before attempting to save
            if not self.validate_transaction_data(tx):
                logger.debug(f"Skipping invalid transaction: {tx.get('transaction_hash', 'unknown')}")
                continue
            
            try:
                cur = self.db_connection.cursor()
                
                query = """
                    INSERT INTO whale_activities (
                        symbol, transaction_hash, whale_address, amount_usd, transaction_type, detected_at
                    ) VALUES (
                        %(symbol)s, %(transaction_hash)s, %(whale_address)s, %(amount_usd)s, %(transaction_type)s, %(detected_at)s
                    )
                    ON CONFLICT (transaction_hash) DO NOTHING;
                """
                
                cur.execute(query, tx)
                
                if cur.rowcount > 0:
                    saved_count += 1
                
                # Commit each transaction individually to avoid cascade failures
                self.db_connection.commit()
                
            except IntegrityError as e:
                logger.debug(f"Integrity error (likely duplicate): {str(e)[:100]}")
                self.db_connection.rollback()
                
            except DataError as e:
                logger.warning(f"Data error: {str(e)[:100]}")
                self.db_connection.rollback()
                
            except Exception as e:
                logger.warning(f"Database error: {str(e)[:100]}")
                self.db_connection.rollback()
            
            finally:
                if 'cur' in locals():
                    cur.close()
        
        logger.info(f"💾 Saved {saved_count}/{len(transactions)} whale transactions")
        return saved_count
    
    def scan_token_whales(self, symbol, token_info, token_price, start_block, end_block):
        """Scan for whale transactions in a token"""
        if token_price <= 0:
            logger.warning(f"Skipping {symbol} - no price data")
            return []
        
        logger.info(f"🔍 Scanning {symbol} (${token_price:.6f})...")
        
        transfers = self.etherscan.get_token_transfers(
            token_info['address'], start_block, end_block
        )
        
        if not transfers:
            logger.info(f"  No transfers found for {symbol}")
            return []
        
        whale_transactions = []
        
        for transfer in transfers:
            try:
                # Extract basic transfer data
                tx_hash = transfer.get('hash')
                from_addr = transfer.get('from', '').lower()
                to_addr = transfer.get('to', '').lower()
                raw_amount = transfer.get('value', '0')
                
                if not tx_hash or not from_addr or not to_addr or raw_amount == '0':
                    continue
                
                # Calculate token amount (handle decimals properly)
                try:
                    raw_value = float(raw_amount)
                    token_amount = raw_value / (10 ** token_info['decimals'])
                    usd_amount = token_amount * token_price
                except (ValueError, TypeError, ZeroDivisionError):
                    continue
                
                # Check whale threshold
                if usd_amount < WHALE_THRESHOLD_USD or usd_amount > MAX_USD_AMOUNT:
                    continue
                
                # Create transaction record
                whale_tx = {
                    'symbol': symbol,
                    'transaction_hash': tx_hash,
                    'whale_address': to_addr,  # Receiver is the whale
                    'amount_usd': round(usd_amount, 2),
                    'transaction_type': 'buy',  # Large incoming transfer = buy
                    'detected_at': datetime.utcnow()
                }
                
                whale_transactions.append(whale_tx)
                
            except Exception as e:
                logger.debug(f"Error processing {symbol} transfer: {e}")
                continue
        
        if whale_transactions:
            logger.info(f"  🐋 Found {len(whale_transactions)} {symbol} whales")
        
        return whale_transactions
    
    def run_scan(self):
        """Execute whale scan - CRON OPTIMIZED (no time checks, runs immediately)"""
        print("🔧 RUN_SCAN: Function started", flush=True)
        logger.info("🎯 FCB WHALE DISCOVERY SCANNER v5.0 - CRON MODE STARTING")
        sys.stdout.flush()
        start_time = datetime.utcnow()
        
        print("🔧 RUN_SCAN: About to connect to database", flush=True)
        if not self.connect_database():
            logger.error("❌ Database connection failed - exiting")
            return False
        
        try:
            # Get latest block
            latest_block = self.etherscan.get_latest_block()
            if latest_block <= 0:
                logger.error("❌ Cannot determine latest block - exiting")
                return False
            
            # Calculate 6-hour scan range
            blocks_per_hour = 300  # ~12 seconds per block
            blocks_back = blocks_per_hour * 6
            start_block = max(0, latest_block - blocks_back)
            
            logger.info(f"📊 Scanning blocks {start_block:,} to {latest_block:,}")
            
            # Get token prices
            coingecko_ids = [info['coingecko_id'] for info in TOP_TOKENS.values()]
            prices = self.coingecko.get_multiple_prices(coingecko_ids)
            
            if not prices:
                logger.error("❌ No token prices retrieved - exiting")
                return False
            
            # Scan each token
            total_whales = 0
            total_volume = 0.0
            
            for symbol, token_info in TOP_TOKENS.items():
                try:
                    price = prices.get(token_info['coingecko_id'], 0)
                    
                    if price <= 0:
                        logger.warning(f"No price for {symbol}, skipping")
                        continue
                    
                    whales = self.scan_token_whales(
                        symbol, token_info, price, start_block, latest_block
                    )
                    
                    if whales:
                        saved = self.save_transactions(whales)
                        volume = sum(tx['amount_usd'] for tx in whales)
                        
                        total_whales += saved
                        total_volume += volume
                        
                        logger.info(f"  ✅ {symbol}: {saved} whales, ${volume:,.0f} volume")
                    else:
                        logger.info(f"  ⚪ {symbol}: No whales found")
                    
                except Exception as e:
                    logger.error(f"❌ Error scanning {symbol}: {e}")
                    continue
            
            # Session summary
            duration = (datetime.utcnow() - start_time).total_seconds() / 60
            
            logger.info("🎉 WHALE SCAN COMPLETE!")
            logger.info(f"  🐋 Total whales found: {total_whales}")
            logger.info(f"  💰 Total volume: ${total_volume:,.2f}")
            logger.info(f"  ⏰ Duration: {duration:.1f} minutes")
            logger.info(f"  📊 Tokens scanned: {len(TOP_TOKENS)}")
            sys.stdout.flush()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Scan failed: {e}")
            return False
        
        finally:
            if self.db_connection:
                self.db_connection.close()
                logger.info("📝 Database connection closed")
                sys.stdout.flush()

def main():
    """Main entry point for cron execution"""
    print("🔧 MAIN: Starting main function", flush=True)
    
    try:
        print("🔧 MAIN: Creating WhaleScanner instance", flush=True)
        scanner = WhaleScanner()
        
        print("🔧 MAIN: Starting run_scan", flush=True)
        success = scanner.run_scan()
        
    except Exception as e:
        print(f"🔧 MAIN: Exception caught: {e}", flush=True)
        logger.error(f"❌ Main function failed: {e}")
        sys.stderr.flush()
        exit(1)
    
    if success:
        logger.info("✅ Whale scanner completed successfully")
        sys.stdout.flush()
        exit(0)  # Success exit code
    else:
        logger.error("❌ Whale scanner failed")
        sys.stderr.flush()
        exit(1)  # Error exit code

if __name__ == "__main__":
    main()
