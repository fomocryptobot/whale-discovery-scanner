#!/usr/bin/env python3
"""
FCB Whale Discovery Scanner v4 - FIXED VERSION
Runs every 6 hours to discover new whale transactions
Includes proper decimal handling, validation, and correct CoinGecko Pro API integration
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

# Configuration from environment variables
DB_URL = os.getenv('DB_URL', 'postgresql://wallet_admin:AbRD14errRCD6H793FRCcPvXIRLgNugK@dpg-d1vd05je5dus739m8mv0-a.frankfurt-postgres.render.com:5432/wallet_transactions')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'GCB4J11T34YG29GNJJX7R7JADRTAFJKPDE')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY', 'CG-bJP1bqyMemFNQv5dp4nvA9xm')

# Scanner settings
WHALE_THRESHOLD_USD = 1000
ETHERSCAN_DELAY = 0.6  # 0.6 seconds for live scanning
COINGECKO_DELAY = 0.12  # Pro API: 500 calls/min = 0.12s delay minimum
MAX_USD_AMOUNT = 100_000_000  # $100M sanity check
SCANNER_VERSION = "whale_discovery_v4"

# CoinGecko Pro API settings
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"
COINGECKO_PRO_HEADERS = {'x-cg-pro-api-key': COINGECKO_API_KEY}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Established tokens for live scanning (54 tokens)
ESTABLISHED_TOKENS = {
    # DeFi tokens
    'UNI': {'address': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18, 'coingecko_id': 'uniswap'},
    'LINK': {'address': '0x514910771af9ca656af840dff83e8264ecf986ca', 'decimals': 18, 'coingecko_id': 'chainlink'},
    'AAVE': {'address': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'decimals': 18, 'coingecko_id': 'aave'},
    'COMP': {'address': '0xc00e94cb662c3520282e6f5717214004a7f26888', 'decimals': 18, 'coingecko_id': 'compound-governance-token'},
    'CRV': {'address': '0xd533a949740bb3306d119cc777fa900ba034cd52', 'decimals': 18, 'coingecko_id': 'curve-dao-token'},
    'SUSHI': {'address': '0x6b3595068778dd592e39a122f4f5a5cf09c90fe2', 'decimals': 18, 'coingecko_id': 'sushi'},
    'MKR': {'address': '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', 'decimals': 18, 'coingecko_id': 'maker'},
    'LDO': {'address': '0x5a98fcbea516cf06857215779fd812ca3bef1b32', 'decimals': 18, 'coingecko_id': 'lido-dao'},
    'SNX': {'address': '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f', 'decimals': 18, 'coingecko_id': 'havven'},
    'YFI': {'address': '0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e', 'decimals': 18, 'coingecko_id': 'yearn-finance'},
    
    # Stablecoins
    'USDC': {'address': '0xa0b86a33e42441e6a2cc5a9c13a9f0b1c8b33e9a4', 'decimals': 6, 'coingecko_id': 'usd-coin'},
    'USDT': {'address': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'decimals': 6, 'coingecko_id': 'tether'},
    'DAI': {'address': '0x6b175474e89094c44da98b954eedeac495271d0f', 'decimals': 18, 'coingecko_id': 'dai'},
    'BUSD': {'address': '0x4fabb145d64652a948d72533023f6e7a623c7c53', 'decimals': 18, 'coingecko_id': 'binance-usd'},
    'LUSD': {'address': '0x5f98805a4e8be255a32880fdec7f6728c6568ba0', 'decimals': 18, 'coingecko_id': 'liquity-usd'},
    
    # Layer 2
    'MATIC': {'address': '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0', 'decimals': 18, 'coingecko_id': 'matic-network'},
    'ARB': {'address': '0xb50721bcf8d664c30412cfbc6cf7a15145234ad1', 'decimals': 18, 'coingecko_id': 'arbitrum'},
    'IMX': {'address': '0xf57e7e7c23978c3caec3c3548e3d615c346e79ff', 'decimals': 18, 'coingecko_id': 'immutable-x'},
    'LRC': {'address': '0xbbbbca6a901c926f240b89eacb641d8aec7aeafd', 'decimals': 18, 'coingecko_id': 'loopring'},
    
    # Gaming
    'APE': {'address': '0x4d224452801aced8b2f0aebe155379bb5d594381', 'decimals': 18, 'coingecko_id': 'apecoin'},
    'SAND': {'address': '0x3845badade8e6dff049820680d1f14bd3903a5d0', 'decimals': 18, 'coingecko_id': 'the-sandbox'},
    'MANA': {'address': '0x0f5d2fb29fb7d3cfee444a200298f468908cc942', 'decimals': 18, 'coingecko_id': 'decentraland'},
    'AXS': {'address': '0xbb0e17ef65f82ab018d8edd776e8dd940327b28b', 'decimals': 18, 'coingecko_id': 'axie-infinity'},
    'ENJ': {'address': '0xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c', 'decimals': 18, 'coingecko_id': 'enjincoin'},
    'GALA': {'address': '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da', 'decimals': 8, 'coingecko_id': 'gala'},
    
    # Memes
    'PEPE': {'address': '0x6982508145454ce325ddbe47a25d4ec3d2311933', 'decimals': 18, 'coingecko_id': 'pepe'},
    'SHIB': {'address': '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', 'decimals': 18, 'coingecko_id': 'shiba-inu'},
    'FLOKI': {'address': '0xcf0c122c6b73ff809c693db761e7baebe62b6a2e', 'decimals': 9, 'coingecko_id': 'floki'},
    'BONK': {'address': '0x1151cb3d861920e07a38e03eead12c32178567f6', 'decimals': 5, 'coingecko_id': 'bonk'},
}

class CoinGeckoProAPI:
    """Handles CoinGecko Pro API calls with proper authentication and rate limiting"""
    
    def __init__(self, api_key, delay=COINGECKO_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = COINGECKO_PRO_BASE_URL
        self.headers = {'x-cg-pro-api-key': self.api_key}
        self.price_cache = {}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def _make_request(self, endpoint, params=None, max_retries=3):
        """Make rate-limited request with retries"""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                # Rate limiting - Pro API allows 500-1000 calls/min
                time.sleep(self.delay)
                
                response = self.session.get(url, params=params or {}, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    logger.warning(f"Rate limited, backing off (attempt {attempt + 1})")
                    time.sleep(self.delay * (2 ** attempt))  # Exponential backoff
                    continue
                else:
                    logger.warning(f"HTTP {response.status_code}: {response.text}")
                    return None
                    
            except requests.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(self.delay * (2 ** attempt))
        
        return None
    
    def get_token_price(self, coingecko_id, vs_currency='usd'):
        """Get current token price using Pro API"""
        try:
            # Check cache first (cache for 5 minutes)
            cache_key = f"{coingecko_id}_{vs_currency}"
            now = datetime.utcnow()
            
            if cache_key in self.price_cache:
                cached_time, cached_price = self.price_cache[cache_key]
                if (now - cached_time).total_seconds() < 300:  # 5 minutes
                    return cached_price
            
            # Make API request
            params = {
                'ids': coingecko_id,
                'vs_currencies': vs_currency,
                'include_24hr_change': 'false'
            }
            
            data = self._make_request('simple/price', params)
            
            if data and coingecko_id in data:
                price = data[coingecko_id].get(vs_currency, 0)
                if price > 0:
                    # Cache the result
                    self.price_cache[cache_key] = (now, price)
                    logger.debug(f"Got price for {coingecko_id}: ${price:.8f}")
                    return price
            
            logger.warning(f"No price data for {coingecko_id}")
            return 0
            
        except Exception as e:
            logger.error(f"Price lookup error for {coingecko_id}: {e}")
            return 0
    
    def get_multiple_prices(self, coingecko_ids, vs_currency='usd'):
        """Get multiple token prices in one request (more efficient)"""
        try:
            if not coingecko_ids:
                return {}
            
            # Join up to 250 IDs (API limit)
            ids_string = ','.join(coingecko_ids[:250])
            
            params = {
                'ids': ids_string,
                'vs_currencies': vs_currency,
                'include_24hr_change': 'false'
            }
            
            data = self._make_request('simple/price', params)
            
            if data:
                prices = {}
                now = datetime.utcnow()
                
                for coin_id, price_data in data.items():
                    price = price_data.get(vs_currency, 0)
                    if price > 0:
                        prices[coin_id] = price
                        # Cache individual prices
                        cache_key = f"{coin_id}_{vs_currency}"
                        self.price_cache[cache_key] = (now, price)
                
                logger.info(f"Got prices for {len(prices)} tokens")
                return prices
            
            return {}
            
        except Exception as e:
            logger.error(f"Multiple price lookup error: {e}")
            return {}

class EtherscanAPI:
    """Handles Etherscan API calls with proper rate limiting"""
    
    def __init__(self, api_key, delay=ETHERSCAN_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = "https://api.etherscan.io/api"
        self.session = requests.Session()
    
    def _make_request(self, params, max_retries=3):
        """Make rate-limited request with retries"""
        params['apikey'] = self.api_key
        
        for attempt in range(max_retries):
            try:
                time.sleep(self.delay)  # Rate limiting
                
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('status') == '1':
                    return data.get('result', [])
                else:
                    error_msg = data.get('message', 'Unknown error')
                    if 'rate limit' in error_msg.lower():
                        logger.warning(f"Etherscan rate limited, backing off")
                        time.sleep(self.delay * 2)
                        continue
                    else:
                        logger.warning(f"Etherscan API error: {error_msg}")
                        return []
                    
            except requests.RequestException as e:
                logger.error(f"Etherscan request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(self.delay * (2 ** attempt))
        
        return []
    
    def get_latest_block(self):
        """Get the latest block number"""
        params = {
            'module': 'proxy',
            'action': 'eth_blockNumber'
        }
        
        result = self._make_request(params)
        if result:
            try:
                return int(result, 16)  # Convert hex to int
            except (ValueError, TypeError):
                pass
        
        return 0
    
    def get_token_transfers(self, contract_address, start_block, end_block, page=1, offset=10000):
        """Get ERC-20 token transfers for a contract"""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': contract_address,
            'startblock': start_block,
            'endblock': end_block,
            'page': page,
            'offset': offset,
            'sort': 'desc'
        }
        
        return self._make_request(params)

class WhaleTransactionValidator:
    """Validates whale transactions before database insertion"""
    
    @staticmethod
    def validate_address(address):
        """Validate Ethereum address format"""
        if not address or not isinstance(address, str):
            return None
        
        address = address.strip().lower()
        if not address.startswith('0x') or len(address) != 42:
            return None
        
        try:
            int(address[2:], 16)  # Check if hex
            return address
        except ValueError:
            return None
    
    @staticmethod
    def validate_usd_amount(amount_usd):
        """Validate USD amount is realistic for whale detection"""
        try:
            amount = float(amount_usd)
            
            if amount < WHALE_THRESHOLD_USD:
                return None
                
            if amount > MAX_USD_AMOUNT:
                logger.warning(f"Amount ${amount:,.2f} above sanity limit")
                return None
                
            return amount
            
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def calculate_token_amount(raw_amount, decimals):
        """Convert raw token amount to human-readable format"""
        try:
            raw = float(raw_amount)
            dec = int(decimals) if decimals else 18
            
            human_readable = raw / (10 ** dec)
            return human_readable
            
        except (ValueError, TypeError, ZeroDivisionError):
            return None

class WhaleDiscoveryScanner:
    """Main whale discovery scanner class"""
    
    def __init__(self):
        self.validator = WhaleTransactionValidator()
        self.etherscan = EtherscanAPI(ETHERSCAN_API_KEY)
        self.coingecko = CoinGeckoProAPI(COINGECKO_API_KEY)
        self.db_connection = None
        
    def connect_database(self):
        """Connect to the whale intelligence database"""
        try:
            self.db_connection = psycopg2.connect(DB_URL)
            logger.info("✅ Connected to whale intelligence database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_scan_range(self, hours_back=6):
        """Get block range for the last N hours"""
        try:
            latest_block = self.etherscan.get_latest_block()
            if latest_block == 0:
                logger.error("Failed to get latest block")
                return None, None
            
            # Ethereum averages ~12 seconds per block
            blocks_per_hour = 300  # 3600 seconds / 12 seconds
            blocks_back = blocks_per_hour * hours_back
            
            start_block = max(0, latest_block - blocks_back)
            
            logger.info(f"📊 Scanning blocks {start_block:,} to {latest_block:,} ({hours_back}h back)")
            return start_block, latest_block
            
        except Exception as e:
            logger.error(f"Error calculating scan range: {e}")
            return None, None
    
    def get_token_prices(self):
        """Get current prices for all established tokens"""
        logger.info("💰 Fetching token prices from CoinGecko Pro API...")
        
        coingecko_ids = [info['coingecko_id'] for info in ESTABLISHED_TOKENS.values()]
        prices = self.coingecko.get_multiple_prices(coingecko_ids)
        
        token_prices = {}
        for symbol, info in ESTABLISHED_TOKENS.items():
            coingecko_id = info['coingecko_id']
            price = prices.get(coingecko_id, 0)
            token_prices[symbol] = price
            
            if price > 0:
                logger.info(f"  💰 {symbol}: ${price:.8f}")
            else:
                logger.warning(f"  ⚠️ {symbol}: No price data")
        
        return token_prices
    
    def scan_token_whales(self, symbol, token_info, token_price, start_block, end_block):
        """Scan for whale transactions in a specific token"""
        logger.info(f"🔍 Scanning {symbol} whales...")
        
        if token_price <= 0:
            logger.warning(f"Skipping {symbol} - no price data")
            return []
        
        contract_address = token_info['address']
        decimals = token_info['decimals']
        
        # Get token transfers from Etherscan
        transfers = self.etherscan.get_token_transfers(
            contract_address, start_block, end_block
        )
        
        if not transfers:
            logger.info(f"  No transfers found for {symbol}")
            return []
        
        whale_transactions = []
        
        for transfer in transfers:
            try:
                # Extract transfer data
                tx_hash = transfer.get('hash')
                from_address = self.validator.validate_address(transfer.get('from'))
                to_address = self.validator.validate_address(transfer.get('to'))
                raw_amount = transfer.get('value', '0')
                block_number = int(transfer.get('blockNumber', 0))
                timestamp = datetime.fromtimestamp(int(transfer.get('timeStamp', 0)))
                
                if not tx_hash or not from_address or not to_address or raw_amount == '0':
                    continue
                
                # Calculate human-readable token amount
                token_amount = self.validator.calculate_token_amount(raw_amount, decimals)
                if not token_amount:
                    continue
                
                # Calculate USD value
                usd_amount = token_amount * token_price
                
                # Validate USD amount (whale threshold)
                validated_usd = self.validator.validate_usd_amount(usd_amount)
                if not validated_usd:
                    continue  # Not a whale transaction
                
                # Create whale transaction record
                whale_tx = {
                    'transaction_id': tx_hash,
                    'wallet_address': to_address,  # Receiver is the whale
                    'blockchain': 'ethereum',
                    'block_number': block_number,
                    'block_timestamp': timestamp,
                    'from_address': from_address,
                    'to_address': to_address,
                    'coin_symbol': symbol,
                    'coin_contract': contract_address,
                    'coin_decimals': decimals,
                    'activity_type': 'transfer',  # Can be enhanced later
                    'amount_tokens': token_amount,
                    'amount_usd': validated_usd,
                    'price_per_token': token_price,
                    'raw_transaction': json.dumps(transfer),
                    'data_source': SCANNER_VERSION,
                    'processed_at': datetime.utcnow()
                }
                
                whale_transactions.append(whale_tx)
                
            except Exception as e:
                logger.error(f"Error processing {symbol} transfer: {e}")
                continue
        
        if whale_transactions:
            logger.info(f"  🐋 Found {len(whale_transactions)} {symbol} whales")
        
        return whale_transactions
    
    def save_whale_transactions(self, whale_transactions):
        """Save whale transactions to database"""
        if not whale_transactions or not self.db_connection:
            return 0
        
        try:
            cur = self.db_connection.cursor()
            
            insert_query = """
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
            
            saved_count = 0
            for whale_tx in whale_transactions:
                try:
                    cur.execute(insert_query, whale_tx)
                    if cur.rowcount > 0:
                        saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving transaction: {e}")
                    continue
            
            self.db_connection.commit()
            return saved_count
            
        except Exception as e:
            logger.error(f"Database save error: {e}")
            return 0
    
    def run_discovery_session(self):
        """Run a complete whale discovery session"""
        logger.info("🎯 FCB WHALE DISCOVERY SCANNER v4 - STARTING SESSION")
        session_start = datetime.utcnow()
        
        # Connect to database
        if not self.connect_database():
            logger.error("Cannot proceed without database connection")
            return
        
        try:
            # Get scan range (last 6 hours)
            start_block, end_block = self.get_scan_range(hours_back=6)
            if not start_block or not end_block:
                logger.error("Cannot determine scan range")
                return
            
            # Get token prices
            token_prices = self.get_token_prices()
            
            # Scan each established token
            total_whales = 0
            total_volume = 0
            
            for symbol, token_info in ESTABLISHED_TOKENS.items():
                try:
                    token_price = token_prices.get(symbol, 0)
                    
                    whales = self.scan_token_whales(
                        symbol, token_info, token_price, start_block, end_block
                    )
                    
                    if whales:
                        saved_count = self.save_whale_transactions(whales)
                        
                        whale_volume = sum(tx['amount_usd'] for tx in whales)
                        total_whales += saved_count
                        total_volume += whale_volume
                        
                        logger.info(f"  ✅ {symbol}: {saved_count} whales, ${whale_volume:,.0f} volume")
                
                except Exception as e:
                    logger.error(f"Error scanning {symbol}: {e}")
                    continue
            
            # Session summary
            session_duration = (datetime.utcnow() - session_start).total_seconds() / 60
            
            logger.info("🎉 WHALE DISCOVERY SESSION COMPLETE!")
            logger.info(f"  🐋 Total whales found: {total_whales}")
            logger.info(f"  💰 Total volume: ${total_volume:,.2f}")
            logger.info(f"  ⏰ Session duration: {session_duration:.1f} minutes")
            logger.info(f"  📊 Tokens scanned: {len(ESTABLISHED_TOKENS)}")
            
        except Exception as e:
            logger.error(f"Session error: {e}")
        
        finally:
            if self.db_connection:
                self.db_connection.close()

def main():
    """Main entry point for the whale discovery scanner"""
    scanner = WhaleDiscoveryScanner()
    scanner.run_discovery_session()

if __name__ == "__main__":
    main()
