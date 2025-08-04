#!/usr/bin/env python3
"""
FCB Whale Discovery Scanner v5.0 - CRON OPTIMIZED
Converted from service to cron job - runs immediately and exits cleanly
"""

import sys
import os

# IMMEDIATE DEBUG - before anything else
print("🔧 STARTUP: Script starting...", flush=True)
sys.stdout.flush()
os.environ['PYTHONUNBUFFERED'] = '1'
print("🔧 STARTUP: Unbuffered mode set", flush=True)

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

print("🔧 STARTUP: Modules imported", flush=True)

# RENDER CACHE BUST v6.0 - 2025-07-25-01-20-UTC
# FORCE COMPLETE REBUILD - SCHEMA FIXED, DEBUGGING ENABLED
print("🔥 CACHE BUST v6.0 LOADING...", flush=True)

# Configuration - ALL KEYS FROM ENVIRONMENT VARIABLES
DB_URL = os.getenv('TRINITY_DATABASE_URL')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_PRIVATE_KEY = os.getenv('KRAKEN_PRIVATE_KEY')

print("🔧 STARTUP: Environment variables loaded", flush=True)

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

print("🔧 STARTUP: Environment variables validated", flush=True)

WHALE_THRESHOLD_USD = 1000
ETHERSCAN_DELAY = 0.1  # 10 calls/second for sequential whale scanning
COINGECKO_DELAY = 0.15
MAX_USD_AMOUNT = 100_000_000
# RENDER FORCE REBUILD - 2025-07-25-00-40-UTC - JSONB FIX FINAL
SCANNER_VERSION = "whale_discovery_v6.0_CACHE_BUST"
# CoinGecko Pro API
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

# Additional stdout configuration
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

def fetch_kraken_tradeable_symbols():
    """Fetch all tradeable symbols from Kraken Universe Scanner API"""
    try:
        kraken_api_url = "https://kraken-scanner-webservice.onrender.com/tradeable/coins"
        response = requests.get(kraken_api_url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            coins = data.get('coins', [])
            
            logger.info(f"✅ Fetched {len(coins)} tradeable symbols from Kraken API")
            
            # Convert to format expected by scanner
            token_dict = {}
            for coin in coins:
                symbol = coin.get('symbol', '').upper()
                if symbol:
                    # Try to get contract info for any symbol - no hardcoded filter
                    contract_info = get_ethereum_contract_info(symbol)
                    if contract_info:
                        token_dict[symbol] = contract_info
            
            logger.info(f"📊 Mapped {len(token_dict)} Ethereum tokens for whale scanning")
            return token_dict
            
        else:
            logger.error(f"❌ Kraken API error: HTTP {response.status_code}")
            return {}
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch Kraken symbols: {e}")
        return {}

def get_ethereum_contract_info(symbol):
    """Get Ethereum contract address and info for symbol - expanded mapping from other scanners"""
    # Comprehensive Ethereum contract addresses from all 5 whale scanners
    ethereum_contracts = {
        # Blue-chip foundation tokens
        'WETH': {'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'decimals': 18, 'coingecko_id': 'weth'},
        'USDT': {'address': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'decimals': 6, 'coingecko_id': 'tether'},
        'USDC': {'address': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'decimals': 6, 'coingecko_id': 'usd-coin'},
        'WBTC': {'address': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'decimals': 8, 'coingecko_id': 'wrapped-bitcoin'},
        'UNI': {'address': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18, 'coingecko_id': 'uniswap'},
        'LINK': {'address': '0x514910771af9ca656af840dff83e8264ecf986ca', 'decimals': 18, 'coingecko_id': 'chainlink'},
        'AAVE': {'address': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'decimals': 18, 'coingecko_id': 'aave'},
        
        # DeFi protocol tokens (from Whale DeFi scanner)
        'COMP': {'address': '0xc00e94Cb662C3520282E6f5717214004A7f26888', 'decimals': 18, 'coingecko_id': 'compound-governance-token'},
        'MKR': {'address': '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2', 'decimals': 18, 'coingecko_id': 'maker'},
        'SNX': {'address': '0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F', 'decimals': 18, 'coingecko_id': 'havven'},
        'CRV': {'address': '0xD533a949740bb3306d119CC777fa900bA034cd52', 'decimals': 18, 'coingecko_id': 'curve-dao-token'},
        'SUSHI': {'address': '0x6B3595068778DD592e39A122f4f5a5cF09C90fE2', 'decimals': 18, 'coingecko_id': 'sushi'},
        'LDO': {'address': '0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32', 'decimals': 18, 'coingecko_id': 'lido-dao'},
        'FRAX': {'address': '0x853d955aCEf822Db058eb8505911ED77F175b99e', 'decimals': 18, 'coingecko_id': 'frax'},
        'CVX': {'address': '0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B', 'decimals': 18, 'coingecko_id': 'convex-finance'},
        
        # Layer1 protocol tokens (from Whale Layer1 scanner)
        'MATIC': {'address': '0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0', 'decimals': 18, 'coingecko_id': 'matic-network'},
        'AVAX': {'address': '0x85f138bfEE4ef8e540890CFb48F620571d67Eda3', 'decimals': 18, 'coingecko_id': 'avalanche-2'},
        'ATOM': {'address': '0x8D983cb9388EaC77af0474fA441C4815500Cb7BB', 'decimals': 6, 'coingecko_id': 'cosmos'},
        'NEAR': {'address': '0x85F17Cf997934a597031b2E18a9aB6ebD4B9f6a4', 'decimals': 24, 'coingecko_id': 'near'},
        'FTM': {'address': '0x4E15361FD6b4BB609Fa63C81A2be19d873717870', 'decimals': 18, 'coingecko_id': 'fantom'},
        'ALGO': {'address': '0x27702a26126e0B3702af63Ee09aC4d1A084EF628', 'decimals': 6, 'coingecko_id': 'algorand'},
        'XTZ': {'address': '0x2e3D870790dC77A83DD1d18184Acc7439A53f475', 'decimals': 18, 'coingecko_id': 'tezos'},
        'EOS': {'address': '0x86Fa049857E0209aa7D9e616F7eb3b3B78ECfdb0', 'decimals': 18, 'coingecko_id': 'eos'},
        'THETA': {'address': '0x3883f5e181fccaF8410FA61e12b59BAd963fb645', 'decimals': 18, 'coingecko_id': 'theta-token'},
        'ONE': {'address': '0xD5cd84D6f044AbE314Ee7E414d37cae8773ef9D3', 'decimals': 18, 'coingecko_id': 'harmony'},
        
        # Exchange tokens (from Whale Exchanges scanner)  
        'BNB': {'address': '0xB8c77482e0A65C1D1d166A4D0eFE54F5F06c40C5', 'decimals': 18, 'coingecko_id': 'binancecoin'},
        'CRO': {'address': '0xA0b73E1Ff0B80914AB6fe0444E65848C4C34450b', 'decimals': 8, 'coingecko_id': 'crypto-com-chain'},
        'LEO': {'address': '0x2AF5D2aD76741191D15Dfe7bF6aC92d4Bd912Ca3', 'decimals': 18, 'coingecko_id': 'leo-token'},
        'FTT': {'address': '0x50D1c9771902476076eCFc8B2A83Ad6b9355a4c9', 'decimals': 18, 'coingecko_id': 'ftx-token'},
        'HT': {'address': '0x6f259637dcD74C767781E37Bc6133cd6A68aa161', 'decimals': 18, 'coingecko_id': 'huobi-token'},
        'KCS': {'address': '0xf34960d9d60be18cC1D5Afc1A6F012A723a28811', 'decimals': 6, 'coingecko_id': 'kucoin-shares'},
        'GT': {'address': '0xE66747a101bFF2dBA3697199DCcE5b743b454759', 'decimals': 18, 'coingecko_id': 'gatechain-token'},
        'MX': {'address': '0x11eEf04c884E24d9B7B4760e7476D06ddF797f36', 'decimals': 18, 'coingecko_id': 'mexc-token'},
        'OKB': {'address': '0x75231F58b43240C9718Dd58B4967c5114342a86c', 'decimals': 18, 'coingecko_id': 'okb'},
        'NEXO': {'address': '0xB62132e35a6c13ee1EE0f84dC5d40bad8d815206', 'decimals': 18, 'coingecko_id': 'nexo'},
        
        # Retail/meme tokens (from Whale Retail scanner)
        'SHIB': {'address': '0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE', 'decimals': 18, 'coingecko_id': 'shiba-inu'},
        'PEPE': {'address': '0x6982508145454Ce325dDbE47a25d4ec3d2311933', 'decimals': 18, 'coingecko_id': 'pepe'},
        'FLOKI': {'address': '0xcf0C122c6b73ff809C693DB761e7BaeBdF2A2089', 'decimals': 9, 'coingecko_id': 'floki'},
        'APE': {'address': '0x4d224452801ACEd8B2F0aebE155379bb5D594381', 'decimals': 18, 'coingecko_id': 'apecoin'},
        'LUNC': {'address': '0xd2877702675e6cE604938B093525e9100cAf9f3c', 'decimals': 18, 'coingecko_id': 'terra-luna'},
    }
    
    return ethereum_contracts.get(symbol.upper())

# High-value whale intelligence: Known Ethereum tokens (dynamic loading in scanner)
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
        self.tokens_to_scan = self.load_tokens_for_scanning()

    def load_tokens_for_scanning(self):
        """Load tokens dynamically from Kraken API - NO FALLBACK"""
        try:
            dynamic_tokens = fetch_kraken_tradeable_symbols()
            if dynamic_tokens and len(dynamic_tokens) > 0:
                logger.info(f"✅ Loaded {len(dynamic_tokens)} dynamic tokens from Kraken")
                return dynamic_tokens
            else:
                logger.error("❌ No tokens received from Kraken API - ABORTING")
                return {}
        except Exception as e:
            logger.error(f"❌ Failed to load dynamic tokens: {e} - ABORTING")
            return {}
    
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
                logger.debug(f"Skipping invalid transaction: {tx.get('transaction_id', 'unknown')}")
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
                
                # Debug: Check field lengths before insert
                if len(tx.get('blockchain', '')) > 32:
                    logger.warning(f"blockchain too long: {len(tx['blockchain'])} chars")
                if len(tx.get('data_source', '')) > 32:
                    logger.warning(f"data_source too long: {len(tx['data_source'])} chars")
                if len(tx.get('coin_symbol', '')) > 16:
                    logger.warning(f"coin_symbol too long: {len(tx['coin_symbol'])} chars")
                
                cur.execute(query, tx)
                
                if cur.rowcount > 0:
                    saved_count += 1
                else:
                    logger.debug(f"Skipped duplicate: {tx.get('transaction_id', 'unknown')[:16]}...")
                
                # Commit each transaction individually to avoid cascade failures
                self.db_connection.commit()
                
            except IntegrityError as e:
                logger.debug(f"Integrity error (likely duplicate): {str(e)[:100]}")
                self.db_connection.rollback()
                
            except DataError as e:
                logger.warning(f"Data error: {str(e)[:100]}")
                self.db_connection.rollback()
                
            except Exception as e:
                logger.warning(f"Database error: {type(e).__name__}: {str(e)[:200]}")
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
        
        # Track unique transactions in this scan to prevent duplicates
        seen_transactions = set()
        
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
                
                # Check whale threshold
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
                    'data_source': SCANNER_VERSION,
                    'processed_at': datetime.utcnow()
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
        logger.info("🎯 FCB WHALE DISCOVERY SCANNER v6.0 - CACHE BUST MODE STARTING")
        sys.stdout.flush()
        start_time = datetime.utcnow()
        
        print("🔧 RUN_SCAN: About to connect to database", flush=True)
        if not self.connect_database():
            logger.error("❌ Database connection failed - exiting")
            return False
        
        # DEBUG: Check actual database schema
        try:
            cur = self.db_connection.cursor()
            cur.execute("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'whale_transactions' ORDER BY column_name;")
            schema_info = cur.fetchall()
            logger.info("🔍 ACTUAL DATABASE SCHEMA:")
            for col_name, data_type, max_length in schema_info:
                logger.info(f"  {col_name}: {data_type}({max_length})")
            cur.close()
        except Exception as e:
            logger.warning(f"Could not check schema: {e}")
        
        try:
            # Get latest block
            latest_block = self.etherscan.get_latest_block()
            if latest_block <= 0:
                logger.error("❌ Cannot determine latest block - exiting")
                return False
            
            # Calculate 5-minute scan range (sequential scanning optimized)
            blocks_per_hour = 300  # ~12 seconds per block
            blocks_back = int(blocks_per_hour * (5/60))  # 5-minute window = 25 blocks
            start_block = max(0, latest_block - blocks_back)
            
            logger.info(f"📊 Scanning blocks {start_block:,} to {latest_block:,} ({blocks_back} blocks = 5 minutes)")
            
            # Get token prices
            coingecko_ids = [info['coingecko_id'] for info in self.tokens_to_scan.values()]
            prices = self.coingecko.get_multiple_prices(coingecko_ids)
            
            if not prices:
                logger.error("❌ No token prices retrieved - exiting")
                return False
            
            # Scan each token
            total_whales = 0
            total_volume = 0.0
            
            for symbol, token_info in self.tokens_to_scan.items():
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
            logger.info(f"  📊 Tokens scanned: {len(self.tokens_to_scan)}")
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
