#!/usr/bin/env python3
"""
FOMO Universe Trinity - Master Whale Scanner v1.0
=================================================
Single comprehensive whale scanner for ALL 464+ Kraken tokens
ZERO HARDCODED SYMBOLS - Complete dynamic contract discovery

SPECIFICATIONS:
- $500 whale threshold (catches all whale activity)
- 2-minute scan cycles (optimal API utilization)
- 464+ token coverage (full Kraken universe)
- 20 calls/sec Etherscan Advanced Plan
- 500 calls/min CoinGecko Pro Plan
- ZERO hardcoded data - everything discovered dynamically
"""

import sys
import os

# IMMEDIATE DEBUG - before anything else
print("🔧 MASTER WHALE SCANNER: Script starting...", flush=True)
sys.stdout.flush()
os.environ['PYTHONUNBUFFERED'] = '1'
print("🔧 MASTER WHALE SCANNER: Unbuffered mode set", flush=True)

import requests
import time
import json
from datetime import datetime, timedelta
import logging

try:
    import psycopg
    from psycopg import IntegrityError, DataError
except ImportError:
    print("❌ Installing psycopg...", flush=True)
    os.system("pip install psycopg[binary]")
    import psycopg
    from psycopg import IntegrityError, DataError

print("🔧 MASTER WHALE SCANNER: Modules imported", flush=True)

# MASTER SCANNER IDENTIFICATION
SCANNER_NAME = "Master_Whale_Scanner"
SCANNER_VERSION = "master_whale_scanner_v1.0"
SCANNER_SCHEDULE = "every_2_minutes"  # Optimal API utilization

print(f"🔥 {SCANNER_NAME} LOADING...", flush=True)

# Configuration - ALL KEYS FROM ENVIRONMENT VARIABLES
DB_URL = os.getenv('TRINITY_DATABASE_URL')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_PRIVATE_KEY = os.getenv('KRAKEN_PRIVATE_KEY')

print("🔧 MASTER WHALE SCANNER: Environment variables loaded", flush=True)

# Validate required environment variables
if not DB_URL:
    raise ValueError("❌ TRINITY_DATABASE_URL environment variable is required")
if not ETHERSCAN_API_KEY:
    raise ValueError("❌ ETHERSCAN_API_KEY environment variable is required") 
if not COINGECKO_API_KEY:
    raise ValueError("❌ COINGECKO_API_KEY environment variable is required")
if not KRAKEN_API_KEY:
    raise ValueError("❌ KRAKEN_API_KEY environment variable is required")
if not KRAKEN_PRIVATE_KEY:
    raise ValueError("❌ KRAKEN_PRIVATE_KEY environment variable is required")

print("🔧 MASTER WHALE SCANNER: Environment variables validated", flush=True)

# Master scanner configuration - optimized for 2-minute cycles
WHALE_THRESHOLD_USD = 500  # $500 catches ALL whale activity (retail + institutional)
ETHERSCAN_DELAY = 0.05  # 20 calls/second for Advanced Plan (0.05s = 20/sec)
COINGECKO_DELAY = 0.12  # 500 calls/minute = 8.33/sec, use 0.12s for safety
MAX_USD_AMOUNT = 100_000_000
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

# Additional stdout configuration
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Enhanced logging for Master Scanner
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)

# Test log - this should appear in Render logs
logger.info(f"🚀 {SCANNER_NAME} DEPLOYMENT STARTING")
logger.info(f"⏰ Execution time: {datetime.utcnow()}")

class EtherscanAPI:
    """Etherscan API with enhanced rate limiting for 20 calls/sec Advanced Plan"""
    
    def __init__(self, api_key, delay=ETHERSCAN_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = "https://api.etherscan.io/api"
        self.session = requests.Session()
        self.scanner_name = SCANNER_NAME
    
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
                        logger.info(f"✅ {self.scanner_name} latest block: {block_num:,}")
                        sys.stdout.flush()
                        return block_num
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            logger.warning(f"{self.scanner_name} block lookup failed: {e}")
        
        # NO FALLBACK - if API fails, raise error
        raise Exception(f"❌ ERROR: Cannot determine latest block from Etherscan API. Scanner cannot proceed without current block number.")
    
    def get_token_transfers(self, contract_address, start_block, end_block):
        """Get token transfers with enhanced rate limiting for 20 calls/sec"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': contract_address,
            'startblock': start_block,
            'endblock': end_block,
            'page': 1,
            'offset': 500,  # Optimized for 2-minute cycles
            'sort': 'desc',
            'apikey': self.api_key
        }
        
        for attempt in range(3):
            try:
                time.sleep(self.delay)  # 20 calls/sec rate limiting
                response = self.session.get(self.base_url, params=params, timeout=45)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('status') == '1':
                        result = data.get('result', [])
                        if isinstance(result, list):
                            return result
                        else:
                            logger.warning(f"{self.scanner_name} unexpected result type: {type(result)}")
                            return []
                    elif data.get('message') == 'No transactions found':
                        return []
                    else:
                        logger.warning(f"{self.scanner_name} Etherscan API: {data.get('message', 'Unknown error')}")
                        return []
                        
                else:
                    logger.warning(f"{self.scanner_name} HTTP {response.status_code} - attempt {attempt + 1}")
                    
            except Exception as e:
                logger.warning(f"{self.scanner_name} transfer request failed (attempt {attempt + 1}): {e}")
            
            # Backoff before retry
            if attempt < 2:
                time.sleep(self.delay * 2)
        
        return []

class CoinGeckoProAPI:
    """CoinGecko Pro API with proper rate limiting for 500 calls/min"""
    
    def __init__(self, api_key, delay=COINGECKO_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = COINGECKO_PRO_BASE_URL
        self.headers = {'x-cg-pro-api-key': self.api_key}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.scanner_name = SCANNER_NAME
    
    def get_multiple_prices(self, coingecko_ids):
        """Get token prices with enhanced rate limiting"""
        try:
            if not coingecko_ids:
                return {}
            
            # Split into batches for large requests
            batch_size = 100  # CoinGecko limit
            all_prices = {}
            
            for i in range(0, len(coingecko_ids), batch_size):
                batch = coingecko_ids[i:i + batch_size]
                ids_string = ','.join(batch)
                
                params = {
                    'ids': ids_string,
                    'vs_currencies': 'usd'
                }
                
                time.sleep(self.delay)
                response = self.session.get(f"{self.base_url}/simple/price", params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for coin_id, price_data in data.items():
                        price = price_data.get('usd', 0)
                        if price > 0:
                            all_prices[coin_id] = price
                else:
                    logger.warning(f"{self.scanner_name} CoinGecko batch error: HTTP {response.status_code}")
            
            logger.info(f"💰 {self.scanner_name} retrieved prices for {len(all_prices)} tokens")
            return all_prices
            
        except Exception as e:
            logger.error(f"{self.scanner_name} price lookup failed: {e}")
            return {}

class MasterWhaleScanner:
    """Master Whale Scanner - Single scanner for ALL tokens"""
    
    def __init__(self):
        self.etherscan = EtherscanAPI(ETHERSCAN_API_KEY)
        self.coingecko = CoinGeckoProAPI(COINGECKO_API_KEY)
        self.db_connection = None
        self.scanner_name = SCANNER_NAME
        self.tokens_to_scan = self.load_tokens_for_scanning()

    def load_tokens_for_scanning(self):
        """Load tokens from CoinGecko database - NO API calls required"""
        try:
            logger.info(f"🔍 {self.scanner_name} querying CoinGecko database for contract addresses...")
            
            # Query your enhanced CoinGecko database
            contracts_url = "https://coingecko-datacollector.onrender.com/contracts"
            response = requests.get(contracts_url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                contracts = data.get('contracts', {})
                
                if contracts and len(contracts) >= 50:
                    logger.info(f"✅ {self.scanner_name} loaded {len(contracts)} tokens from database")
                    logger.info(f"🚀 {self.scanner_name} eliminated 1000+ API calls - using database only!")
                    return contracts
                else:
                    raise Exception(f"❌ ERROR: Insufficient contracts in database: {len(contracts)}. Need minimum 50 contracts.")
            else:
                raise Exception(f"❌ ERROR: CoinGecko database query failed: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Database query failed: {e}")
            raise Exception(f"❌ CRITICAL ERROR: Cannot load contracts from database - {e}. Master Scanner requires database connection.")
    
    def get_prices_from_database(self):
        """Get current token prices from CoinGecko database"""
        try:
            # Query market data from your database
            conn = psycopg.connect(DB_URL)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT ON (symbol) symbol, current_price
                FROM market_data 
                WHERE current_price > 0
                ORDER BY symbol, collected_at DESC
            """)
            
            prices = {}
            for row in cursor.fetchall():
                symbol = row[0].upper()
                price = float(row[1])
                if symbol in self.tokens_to_scan:
                    prices[self.tokens_to_scan[symbol]['coingecko_id']] = price
            
            cursor.close()
            conn.close()
            
            logger.info(f"💰 {self.scanner_name} retrieved {len(prices)} prices from database")
            return prices
            
        except Exception as e:
            logger.error(f"❌ Database price lookup failed: {e}")
            return {}

    def connect_database(self):
        """Connect to database with autocommit disabled"""
        try:
            self.db_connection = psycopg.connect(DB_URL)
            self.db_connection.autocommit = False  # Enable transaction control
            logger.info(f"✅ {self.scanner_name} database connected")
            sys.stdout.flush()
            return True
        except Exception as e:
            logger.error(f"❌ {self.scanner_name} database connection failed: {e}")
            sys.stderr.flush()
            return False
    
    def validate_transaction_data(self, tx):
        """Validate transaction data before database insert"""
        required_fields = ['transaction_id', 'wallet_address', 'blockchain', 'coin_symbol', 'amount_tokens', 'amount_usd']
        
        try:
            # Check required fields
            for field in required_fields:
                if field not in tx or tx[field] is None or tx[field] == '':
                    return False
            
            # Validate transaction ID format
            if not isinstance(tx['transaction_id'], str) or not tx['transaction_id'].startswith('0x'):
                return False
            
            # Validate wallet address format  
            if not isinstance(tx['wallet_address'], str) or len(tx['wallet_address']) != 42:
                return False
            
            # Validate USD amount range ($500 minimum for master scanner)
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
                logger.debug(f"{self.scanner_name} skipping invalid transaction: {tx.get('transaction_id', 'unknown')}")
                continue
            
            try:
                cur = self.db_connection.cursor()
                
                # First ensure wallet exists
                wallet_query = """
                    INSERT INTO wallet_accounts (wallet_address) 
                    VALUES (%(wallet_address)s) 
                    ON CONFLICT (wallet_address) DO NOTHING;
                """
                cur.execute(wallet_query, {'wallet_address': tx['wallet_address']})
                
                # Then insert whale transaction
                query = """
                    INSERT INTO whale_transactions (
                        transaction_id, wallet_address, blockchain, block_number, block_timestamp,
                        transaction_index, from_address, to_address, gas_used, gas_price,
                        coin_symbol, coin_contract, coin_decimals, activity_type, amount_tokens,
                        amount_usd, price_per_token, raw_transaction, data_source, processed_at
                    ) VALUES (
                        %(transaction_id)s, %(wallet_address)s, %(blockchain)s, %(block_number)s, %(block_timestamp)s,
                        %(transaction_index)s, %(from_address)s, %(to_address)s, %(gas_used)s, %(gas_price)s,
                        %(coin_symbol)s, %(coin_contract)s, %(coin_decimals)s, %(activity_type)s, %(amount_tokens)s,
                        %(amount_usd)s, %(price_per_token)s, %(raw_transaction)s, %(data_source)s, %(processed_at)s
                    )
                    ON CONFLICT (transaction_id) DO NOTHING;
                """
                
                cur.execute(query, tx)
                
                if cur.rowcount > 0:
                    saved_count += 1
                else:
                    logger.debug(f"{self.scanner_name} skipped duplicate: {tx.get('transaction_id', 'unknown')[:16]}...")
                
                # Commit each transaction individually to avoid cascade failures
                self.db_connection.commit()
                
            except IntegrityError as e:
                logger.debug(f"{self.scanner_name} integrity error (likely duplicate): {str(e)[:100]}")
                self.db_connection.rollback()
                
            except DataError as e:
                logger.warning(f"{self.scanner_name} data error: {str(e)[:100]}")
                self.db_connection.rollback()
                
            except Exception as e:
                logger.warning(f"{self.scanner_name} database error: {type(e).__name__}: {str(e)[:200]}")
                self.db_connection.rollback()
            
            finally:
                if 'cur' in locals():
                    cur.close()
        
        logger.info(f"💾 {self.scanner_name} saved {saved_count}/{len(transactions)} whale transactions")
        return saved_count
    
    def scan_token_whales(self, symbol, token_info, token_price, start_block, end_block):
        """Scan for whale transactions in a token with $500 threshold"""
        if token_price <= 0:
            logger.warning(f"{self.scanner_name} skipping {symbol} - no price data")
            return []
        
        logger.info(f"🔍 {self.scanner_name} scanning {symbol} (${token_price:.6f})...")
        
        # Track unique transactions in this scan to prevent duplicates
        seen_transactions = set()
        
        transfers = self.etherscan.get_token_transfers(
            token_info['address'], start_block, end_block
        )
        
        if not transfers:
            logger.info(f"  {self.scanner_name} no transfers found for {symbol}")
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
                
                # Skip if we've already processed this transaction in this scan
                if tx_hash in seen_transactions:
                    continue
                seen_transactions.add(tx_hash)
                
                # Calculate token amount (handle decimals properly)
                try:
                    raw_value = float(raw_amount)
                    token_amount = raw_value / (10 ** token_info['decimals'])
                    usd_amount = token_amount * token_price
                except (ValueError, TypeError, ZeroDivisionError):
                    continue
                
                # Check whale threshold ($500 minimum)
                if usd_amount < WHALE_THRESHOLD_USD or usd_amount > MAX_USD_AMOUNT:
                    continue
                
                # Create transaction record
                whale_tx = {
                    'transaction_id': tx_hash,
                    'wallet_address': to_addr,  # Receiver is the whale
                    'blockchain': 'eth',
                    'block_number': int(transfer.get('blockNumber', 0)) if transfer.get('blockNumber') else None,
                    'block_timestamp': datetime.fromtimestamp(int(transfer.get('timeStamp', 0))),
                    'transaction_index': int(transfer.get('transactionIndex', 0)) if transfer.get('transactionIndex') else None,
                    'from_address': from_addr if from_addr else None,
                    'to_address': to_addr if to_addr else None,
                    'gas_used': int(transfer.get('gasUsed', 0)) if transfer.get('gasUsed') else None,
                    'gas_price': int(transfer.get('gasPrice', 0)) if transfer.get('gasPrice') else None,
                    'transaction_fee_usd': None,  # Calculate if needed
                    'coin_symbol': symbol,
                    'coin_contract': token_info['address'].lower(),
                    'coin_decimals': token_info['decimals'],
                    'activity_type': 'transfer',  # Use lowercase as required
                    'amount_tokens': token_amount,
                    'amount_usd': round(usd_amount, 2),
                    'price_per_token': token_price,
                    'raw_transaction': json.dumps(transfer),  # Convert dict to JSON string for JSONB
                    'data_source': SCANNER_VERSION,  # Master scanner identification
                    'processed_at': datetime.utcnow()
                }
                
                whale_transactions.append(whale_tx)
                
            except Exception as e:
                logger.debug(f"{self.scanner_name} error processing {symbol} transfer: {e}")
                continue
        
        if whale_transactions:
            logger.info(f"  🐋 {self.scanner_name} found {len(whale_transactions)} {symbol} whales")
        
        return whale_transactions
    
    def run_master_scan(self):
        """Execute Master whale scan - 2-minute cycles, all tokens, $500 threshold"""
        print(f"🔧 {self.scanner_name}: Master scan starting", flush=True)
        logger.info(f"🎯 {SCANNER_NAME} MASTER DEPLOYMENT - {SCANNER_VERSION}")
        sys.stdout.flush()
        start_time = datetime.utcnow()
        
        print(f"🔧 {self.scanner_name}: About to connect to database", flush=True)
        if not self.connect_database():
            logger.error(f"❌ {self.scanner_name} database connection failed - mission aborted")
            return False
        
        try:
            # Get latest block
            latest_block = self.etherscan.get_latest_block()
            if latest_block <= 0:
                logger.error(f"❌ {self.scanner_name} cannot determine latest block - mission aborted")
                return False
            
            # Calculate 2-minute scan range (optimized for 2-minute cycles)
            blocks_per_hour = 300  # ~12 seconds per block
            blocks_back = int(blocks_per_hour * (2/60))  # 2-minute window = 10 blocks
            start_block = max(0, latest_block - blocks_back)
            
            logger.info(f"📊 {self.scanner_name} scanning blocks {start_block:,} to {latest_block:,} (2-minute cycle)")
            
            # Get token prices from database instead of API
            prices = self.get_prices_from_database()
            
            if not prices:
                logger.error(f"❌ {self.scanner_name} no token prices retrieved - mission aborted")
                return False
            
            # Scan ALL tokens with $500 threshold
            total_whales = 0
            total_volume = 0.0
            
            for symbol, token_info in self.tokens_to_scan.items():
                try:
                    price = prices.get(token_info['coingecko_id'], 0)
                    
                    if price <= 0:
                        logger.warning(f"{self.scanner_name} no price for {symbol}, skipping")
                        continue
                    
                    whales = self.scan_token_whales(
                        symbol, token_info, price, start_block, latest_block
                    )
                    
                    if whales:
                        saved = self.save_transactions(whales)
                        volume = sum(tx['amount_usd'] for tx in whales)
                        
                        total_whales += saved
                        total_volume += volume
                        
                        logger.info(f"  ✅ {self.scanner_name} {symbol}: {saved} whales, ${volume:,.0f} volume")
                    else:
                        logger.debug(f"  ⚪ {self.scanner_name} {symbol}: No whales found")
                    
                except Exception as e:
                    logger.error(f"❌ {self.scanner_name} error scanning {symbol}: {e}")
                    continue
            
            # Master scanner mission summary
            duration = (datetime.utcnow() - start_time).total_seconds() / 60
            
            logger.info(f"🎉 {SCANNER_NAME} MISSION COMPLETE!")
            logger.info(f"  🐋 Total whales captured: {total_whales}")
            logger.info(f"  💰 Total volume tracked: ${total_volume:,.2f}")
            logger.info(f"  ⏰ Mission duration: {duration:.1f} minutes")
            logger.info(f"  📊 Tokens scanned: {len(self.tokens_to_scan)}")
            logger.info(f"  🔥 Scanner performance: {total_whales/duration:.1f} whales/minute")
            logger.info(f"  💸 Threshold: ${WHALE_THRESHOLD_USD}+ (catches ALL whale activity)")
            sys.stdout.flush()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ {self.scanner_name} master scan failed: {e}")
            return False
        
        finally:
            if self.db_connection:
                self.db_connection.close()
                logger.info(f"📝 {self.scanner_name} database connection closed")
                sys.stdout.flush()

def main():
    """Main entry point for Master Scanner cron execution"""
    print(f"🔧 {SCANNER_NAME}: Starting main function", flush=True)
    
    try:
        print(f"🔧 {SCANNER_NAME}: Creating Master whale scanner instance", flush=True)
        scanner = MasterWhaleScanner()
        
        print(f"🔧 {SCANNER_NAME}: Starting master mission", flush=True)
        success = scanner.run_master_scan()
        
    except Exception as e:
        print(f"🔧 {SCANNER_NAME}: Exception caught: {e}", flush=True)
        logger.error(f"❌ {SCANNER_NAME} main function failed: {e}")
        sys.stderr.flush()
        exit(1)
    
    if success:
        logger.info(f"✅ {SCANNER_NAME} completed mission successfully")
        sys.stdout.flush()
        exit(0)  # Success exit code
    else:
        logger.error(f"❌ {SCANNER_NAME} mission failed")
        sys.stderr.flush()
        exit(1)  # Error exit code

if __name__ == "__main__":
    main()
