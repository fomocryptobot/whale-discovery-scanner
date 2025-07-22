#!/usr/bin/env python3
"""
FCB Whale Discovery Scanner v4.1 - ETHERSCAN FIX
Fixed Etherscan API block number retrieval and added fallbacks
"""

import requests
import time
import json
import os
from datetime import datetime, timedelta
import logging

try:
    import psycopg2
except ImportError:
    print("❌ Installing psycopg2...")
    os.system("pip install psycopg2-binary")
    import psycopg2

# Configuration
DB_URL = os.getenv('DB_URL', 'postgresql://wallet_admin:AbRD14errRCD6H793FRCcPvXIRLgNugK@dpg-d1vd05je5dus739m8mv0-a.frankfurt-postgres.render.com:5432/wallet_transactions')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'GCB4J11T34YG29GNJJX7R7JADRTAFJKPDE')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY', 'CG-bJP1bqyMemFNQv5dp4nvA9xm')

WHALE_THRESHOLD_USD = 1000
ETHERSCAN_DELAY = 0.6
COINGECKO_DELAY = 0.12
MAX_USD_AMOUNT = 100_000_000
SCANNER_VERSION = "whale_discovery_v4.1"

# CoinGecko Pro API
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
COINGECKO_PRO_HEADERS = {'x-cg-pro-api-key': COINGECKO_API_KEY}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Core tokens to scan (reduced list for reliability)
CORE_TOKENS = {
    'UNI': {'address': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18, 'coingecko_id': 'uniswap'},
    'LINK': {'address': '0x514910771af9ca656af840dff83e8264ecf986ca', 'decimals': 18, 'coingecko_id': 'chainlink'},
    'AAVE': {'address': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'decimals': 18, 'coingecko_id': 'aave'},
    'USDC': {'address': '0xa0b86a33e42441e6a2cc5a9c13a9f0b1c8b33e9a4', 'decimals': 6, 'coingecko_id': 'usd-coin'},
    'USDT': {'address': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'decimals': 6, 'coingecko_id': 'tether'},
    'PEPE': {'address': '0x6982508145454ce325ddbe47a25d4ec3d2311933', 'decimals': 18, 'coingecko_id': 'pepe'},
    'SHIB': {'address': '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', 'decimals': 18, 'coingecko_id': 'shiba-inu'},
}

class EtherscanAPI:
    """Etherscan API with improved error handling and fallbacks"""
    
    def __init__(self, api_key, delay=ETHERSCAN_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = "https://api.etherscan.io/api"
        self.session = requests.Session()
    
    def test_connection(self):
        """Test Etherscan API connectivity"""
        logger.info("🔗 Testing Etherscan API connection...")
        
        test_params = {
            'module': 'account',
            'action': 'balance',
            'address': '0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae',
            'tag': 'latest',
            'apikey': self.api_key
        }
        
        try:
            time.sleep(self.delay)
            response = self.session.get(self.base_url, params=test_params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == '1':
                    logger.info("✅ Etherscan API connection working")
                    return True
                else:
                    logger.error(f"❌ Etherscan API error: {data.get('message', 'Unknown')}")
                    return False
            else:
                logger.error(f"❌ HTTP error: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False
    
    def get_latest_block(self):
        """Get latest block number with multiple fallback methods"""
        logger.info("📊 Getting latest block number...")
        
        # Method 1: eth_blockNumber (most reliable)
        try:
            params = {
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': self.api_key
            }
            
            time.sleep(self.delay)
            response = self.session.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'result' in data and data['result']:
                    block_hex = data['result']
                    if isinstance(block_hex, str) and block_hex.startswith('0x'):
                        block_num = int(block_hex, 16)
                        logger.info(f"✅ Latest block from eth_blockNumber: {block_num:,}")
                        return block_num
                    else:
                        logger.warning(f"Unexpected result format: {data}")
                else:
                    logger.warning(f"No result in response: {data}")
                    
        except Exception as e:
            logger.warning(f"Method 1 (eth_blockNumber) failed: {e}")
        
        # Method 2: getblocknobytime (alternative)
        try:
            current_timestamp = int(time.time())
            params = {
                'module': 'block',
                'action': 'getblocknobytime',
                'timestamp': current_timestamp,
                'closest': 'before',
                'apikey': self.api_key
            }
            
            time.sleep(self.delay)
            response = self.session.get(self.base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == '1' and data.get('result'):
                    block_num = int(data['result'])
                    logger.info(f"✅ Latest block from getblocknobytime: {block_num:,}")
                    return block_num
                    
        except Exception as e:
            logger.warning(f"Method 2 (getblocknobytime) failed: {e}")
        
        # Method 3: Estimate based on time (fallback)
        try:
            # Ethereum averages ~12 seconds per block
            # Block 22,000,000 was roughly around late 2024/early 2025
            # Current estimate based on time progression
            
            # Rough baseline: Block 22,000,000 around January 1, 2025
            baseline_timestamp = 1704067200  # Jan 1, 2025 00:00:00 UTC
            baseline_block = 22000000
            
            current_timestamp = int(time.time())
            seconds_elapsed = current_timestamp - baseline_timestamp
            blocks_elapsed = seconds_elapsed // 12  # ~12 seconds per block
            
            estimated_block = baseline_block + blocks_elapsed
            
            # Ensure reasonable bounds
            if estimated_block < 22000000:
                estimated_block = 22000000
            elif estimated_block > 30000000:  # Sanity check
                estimated_block = 25000000
            
            logger.warning(f"⚠️ Using estimated block number: {estimated_block:,}")
            return estimated_block
            
        except Exception as e:
            logger.error(f"Even fallback method failed: {e}")
            
        # Final fallback - use a reasonable recent block
        fallback_block = 22500000
        logger.error(f"🚨 All methods failed, using fallback: {fallback_block:,}")
        return fallback_block
    
    def get_token_transfers(self, contract_address, start_block, end_block, page=1, offset=1000):
        """Get token transfers with improved error handling"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': contract_address,
            'startblock': start_block,
            'endblock': end_block,
            'page': page,
            'offset': offset,
            'sort': 'desc',
            'apikey': self.api_key
        }
        
        for attempt in range(3):
            try:
                time.sleep(self.delay)
                response = self.session.get(self.base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == '1':
                        return data.get('result', [])
                    elif data.get('message') == 'No transactions found':
                        return []
                    else:
                        logger.warning(f"Etherscan API warning: {data.get('message')}")
                        if attempt < 2:
                            time.sleep(self.delay * 2)
                            continue
                        return []
                else:
                    logger.warning(f"HTTP {response.status_code} on attempt {attempt + 1}")
                    if attempt < 2:
                        time.sleep(self.delay * 2)
                        continue
                    
            except Exception as e:
                logger.warning(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    time.sleep(self.delay * 2)
                    continue
        
        return []

class CoinGeckoProAPI:
    """CoinGecko Pro API with proper authentication"""
    
    def __init__(self, api_key, delay=COINGECKO_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = COINGECKO_PRO_BASE_URL
        self.headers = {'x-cg-pro-api-key': self.api_key}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.price_cache = {}
    
    def get_multiple_prices(self, coingecko_ids, vs_currency='usd'):
        """Get multiple token prices efficiently"""
        try:
            if not coingecko_ids:
                return {}
            
            ids_string = ','.join(coingecko_ids[:50])  # Limit batch size
            
            params = {
                'ids': ids_string,
                'vs_currencies': vs_currency,
                'include_24hr_change': 'false'
            }
            
            time.sleep(self.delay)
            response = self.session.get(f"{self.base_url}/simple/price", params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                prices = {}
                for coin_id, price_data in data.items():
                    price = price_data.get(vs_currency, 0)
                    if price > 0:
                        prices[coin_id] = price
                
                logger.info(f"💰 Got prices for {len(prices)} tokens")
                return prices
            else:
                logger.warning(f"CoinGecko API error: HTTP {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Price lookup error: {e}")
            return {}

class WhaleScanner:
    """Main whale scanner class"""
    
    def __init__(self):
        self.etherscan = EtherscanAPI(ETHERSCAN_API_KEY)
        self.coingecko = CoinGeckoProAPI(COINGECKO_API_KEY)
        self.db_connection = None
    
    def connect_database(self):
        """Connect to database"""
        try:
            self.db_connection = psycopg2.connect(DB_URL)
            logger.info("✅ Connected to whale intelligence database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_scan_range(self, hours_back=6):
        """Get block range for scanning"""
        try:
            # Test Etherscan connection first
            if not self.etherscan.test_connection():
                logger.error("Etherscan API test failed")
                return None, None
            
            latest_block = self.etherscan.get_latest_block()
            if latest_block <= 0:
                logger.error("Could not determine latest block")
                return None, None
            
            # Calculate blocks back (Ethereum: ~12 seconds per block)
            blocks_per_hour = 300  # 3600/12
            blocks_back = blocks_per_hour * hours_back
            start_block = max(0, latest_block - blocks_back)
            
            logger.info(f"📊 Scan range: {start_block:,} to {latest_block:,} ({hours_back}h)")
            return start_block, latest_block
            
        except Exception as e:
            logger.error(f"Error getting scan range: {e}")
            return None, None
    
    def scan_token_whales(self, symbol, token_info, token_price, start_block, end_block):
        """Scan for whale transactions in a specific token"""
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
                raw_amount = transfer.get('value', '0')
                if raw_amount == '0':
                    continue
                
                # Calculate human-readable amount
                token_amount = float(raw_amount) / (10 ** token_info['decimals'])
                usd_amount = token_amount * token_price
                
                # Check whale threshold
                if usd_amount < WHALE_THRESHOLD_USD or usd_amount > MAX_USD_AMOUNT:
                    continue
                
                # Create transaction record
                whale_tx = {
                    'transaction_id': transfer.get('hash'),
                    'wallet_address': transfer.get('to', '').lower(),
                    'blockchain': 'ethereum',
                    'block_number': int(transfer.get('blockNumber', 0)),
                    'block_timestamp': datetime.fromtimestamp(int(transfer.get('timeStamp', 0))),
                    'from_address': transfer.get('from', '').lower(),
                    'to_address': transfer.get('to', '').lower(),
                    'coin_symbol': symbol,
                    'coin_contract': token_info['address'],
                    'coin_decimals': token_info['decimals'],
                    'activity_type': 'transfer',
                    'amount_tokens': token_amount,
                    'amount_usd': usd_amount,
                    'price_per_token': token_price,
                    'raw_transaction': json.dumps(transfer),
                    'data_source': SCANNER_VERSION,
                    'processed_at': datetime.utcnow()
                }
                
                whale_transactions.append(whale_tx)
                
            except Exception as e:
                logger.warning(f"Error processing {symbol} transfer: {e}")
                continue
        
        if whale_transactions:
            logger.info(f"  🐋 Found {len(whale_transactions)} {symbol} whales")
        
        return whale_transactions
    
    def save_transactions(self, transactions):
        """Save transactions to database"""
        if not transactions or not self.db_connection:
            return 0
        
        try:
            cur = self.db_connection.cursor()
            
            query = """
                INSERT INTO whale_transactions (
                    transaction_id, wallet_address, blockchain, block_number,
                    block_timestamp, from_address, to_address, coin_symbol,
                    coin_contract, coin_decimals, activity_type, amount_tokens,
                    amount_usd, price_per_token, raw_transaction, data_source,
                    processed_at
                ) VALUES (
                    %(transaction_id)s, %(wallet_address)s, %(blockchain)s, %(block_number)s,
                    %(block_timestamp)s, %(from_address)s, %(to_address)s, %(coin_symbol)s,
                    %(coin_contract)s, %(coin_decimals)s, %(activity_type)s, %(amount_tokens)s,
                    %(amount_usd)s, %(price_per_token)s, %(raw_transaction)s, %(data_source)s,
                    %(processed_at)s
                )
                ON CONFLICT (transaction_id) DO NOTHING;
            """
            
            saved = 0
            for tx in transactions:
                try:
                    cur.execute(query, tx)
                    if cur.rowcount > 0:
                        saved += 1
                except Exception as e:
                    logger.warning(f"Error saving transaction: {e}")
            
            self.db_connection.commit()
            return saved
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return 0
    
    def run_scan(self):
        """Run complete whale scan"""
        logger.info("🎯 FCB WHALE DISCOVERY SCANNER v4.1 - STARTING")
        start_time = datetime.utcnow()
        
        if not self.connect_database():
            return
        
        try:
            # Get scan range
            start_block, end_block = self.get_scan_range(hours_back=6)
            if not start_block:
                logger.error("Cannot proceed without valid scan range")
                return
            
            # Get token prices
            coingecko_ids = [info['coingecko_id'] for info in CORE_TOKENS.values()]
            prices = self.coingecko.get_multiple_prices(coingecko_ids)
            
            # Map prices to tokens
            token_prices = {}
            for symbol, info in CORE_TOKENS.items():
                token_prices[symbol] = prices.get(info['coingecko_id'], 0)
            
            # Scan each token
            total_whales = 0
            total_volume = 0
            
            for symbol, token_info in CORE_TOKENS.items():
                try:
                    price = token_prices.get(symbol, 0)
                    
                    whales = self.scan_token_whales(
                        symbol, token_info, price, start_block, end_block
                    )
                    
                    if whales:
                        saved = self.save_transactions(whales)
                        volume = sum(tx['amount_usd'] for tx in whales)
                        
                        total_whales += saved
                        total_volume += volume
                        
                        logger.info(f"  ✅ {symbol}: {saved} whales, ${volume:,.0f}")
                    
                except Exception as e:
                    logger.error(f"Error scanning {symbol}: {e}")
            
            # Summary
            duration = (datetime.utcnow() - start_time).total_seconds() / 60
            logger.info("🎉 SCAN COMPLETE!")
            logger.info(f"  🐋 Total whales: {total_whales}")
            logger.info(f"  💰 Total volume: ${total_volume:,.2f}")
            logger.info(f"  ⏰ Duration: {duration:.1f}m")
            
        except Exception as e:
            logger.error(f"Scan error: {e}")
        
        finally:
            if self.db_connection:
                self.db_connection.close()

def main():
    scanner = WhaleScanner()
    scanner.run_scan()

if __name__ == "__main__":
    main()
