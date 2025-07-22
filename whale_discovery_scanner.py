#!/usr/bin/env python3
"""
FCB WHALE DISCOVERY SCANNER - 6 HOUR NEW WHALE DETECTION
Continuously discovers new whale wallets every 6 hours
Focuses on finding fresh whale activity and expanding the database
Deploy as Render Background Worker for automated whale discovery
"""

import os
import requests
import psycopg as psycopg2
import json
import time
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

# Database configuration
DATABASE_URL = os.getenv('DB_URL', "postgresql://wallet_admin:AbRD14errRCD6H793FRCcPvXIRLgNugK@dpg-d1vd05je5dus739m8mv0-a.frankfurt-postgres.render.com:5432/wallet_transactions")

# API Keys
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', 'GCB4J11T34YG29GNJJX7R7JADRTAFJKPDE')
COINGECKO_PRO_API_KEY = os.getenv('COINGECKO_API_KEY', 'CG-bJP1bqyMemFNQv5dp4nvA9xm')

# CoinGecko Pro API
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

# Discovery configuration - FCB Session Timing
FCB_SESSION_HOURS = [0, 6, 12, 18]  # UTC hours when FCB sessions begin
DISCOVERY_LEAD_TIME = 30 * 60  # Run 30 minutes before each FCB session
WHALE_THRESHOLD = 1000  # $1000+ transactions
SCAN_HOURS_BACK = 6  # Look back 6 hours each scan (since last FCB session)

# Comprehensive target tokens for whale discovery
target_tokens = {
    'ethereum': {
        # Major DeFi tokens
        'UNI': {'contract': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18},
        'LINK': {'contract': '0x514910771af9ca656af840dff83e8264ecf986ca', 'decimals': 18},
        'AAVE': {'contract': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'decimals': 18},
        'COMP': {'contract': '0xc00e94cb662c3520282e6f5717214004a7f26888', 'decimals': 18},
        'CRV': {'contract': '0xd533a949740bb3306d119cc777fa900ba034cd52', 'decimals': 18},
        'SUSHI': {'contract': '0x6b3595068778dd592e39a122f4f5a5cf09c90fe2', 'decimals': 18},
        
        # Major meme/pump tokens
        'PEPE': {'contract': '0x6982508145454ce325ddbe47a25d4ec3d2311933', 'decimals': 18},
        'SHIB': {'contract': '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', 'decimals': 18},
        'FLOKI': {'contract': '0xcf0c122c6b73ff809c693db761e7baebe62b6a2e', 'decimals': 9},
        'DOGE': {'contract': '0x4206931337dc273a630d328da6441786bfad668f', 'decimals': 8},
        'BONK': {'contract': '0x1151cb3d861920e07a38e03eead12c32178567ecf', 'decimals': 5},
        'WIF': {'contract': '0x76fcfd8e5b1b516a7dc0e2bbe3d6b7c3b1a6bf85', 'decimals': 6},
        
        # Stablecoins (high volume whale activity)
        'USDC': {'contract': '0xa0b86a33e6eb3976d6c5732e4dc9ae7e69b9db0a', 'decimals': 6},
        'USDT': {'contract': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'decimals': 6},
        'DAI': {'contract': '0x6b175474e89094c44da98b954eedeac495271d0f', 'decimals': 18},
        
        # Gaming/NFT tokens
        'APE': {'contract': '0x4d224452801aced8b2f0aebe155379bb5d594381', 'decimals': 18},
        'SAND': {'contract': '0x3845badade8e6dff049820680d1f14bd3903a5d0', 'decimals': 18},
        'MANA': {'contract': '0x0f5d2fb29fb7d3cfee444a200298f468908cc942', 'decimals': 18},
        
        # Layer 2 / Scaling tokens
        'MATIC': {'contract': '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0', 'decimals': 18},
        'OP': {'contract': '0x4200000000000000000000000000000000000042', 'decimals': 18},
        'ARB': {'contract': '0x912ce59144191c1204e64559fe8253a0e49e6548', 'decimals': 18},
    }
}

class WhaleDiscoveryScanner:
    def __init__(self):
        self.last_scan_timestamp = None
        self.total_whales_discovered = 0
        self.scan_count = 0
        
    def get_database_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(DATABASE_URL)
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None
    
    def save_whale_transaction(self, transaction_data):
        """Save whale transaction to database"""
        conn = self.get_database_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT insert_whale_transaction(
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                transaction_data['transaction_id'],
                transaction_data['wallet_address'],
                transaction_data['blockchain'],
                transaction_data['block_number'],
                transaction_data['block_timestamp'],
                transaction_data['from_address'],
                transaction_data['to_address'],
                transaction_data['coin_symbol'],
                transaction_data['activity_type'],
                transaction_data['amount_tokens'],
                transaction_data['amount_usd'],
                json.dumps(transaction_data['raw_transaction']),
                transaction_data['data_source']
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            if "duplicate key" not in str(e).lower():
                print(f"⚠️ Failed to save transaction: {e}")
            return False
        finally:
            conn.close()
    
    def get_next_fcb_session_time(self):
        """Calculate next FCB session time and when to run discovery"""
        now = datetime.utcnow()
        
        # Find next FCB session hour
        current_hour = now.hour
        next_session_hour = None
        
        for session_hour in FCB_SESSION_HOURS:
            if session_hour > current_hour:
                next_session_hour = session_hour
                break
        
        # If no session found today, use first session tomorrow
        if next_session_hour is None:
            next_session_hour = FCB_SESSION_HOURS[0]
            next_session_date = now.date() + timedelta(days=1)
        else:
            next_session_date = now.date()
        
        # Calculate exact session time
        next_session_time = datetime.combine(next_session_date, datetime.min.time().replace(hour=next_session_hour))
        
        # Calculate discovery run time (30 minutes before session)
        discovery_run_time = next_session_time - timedelta(seconds=DISCOVERY_LEAD_TIME)
        
        return discovery_run_time, next_session_time
    
    def get_current_whale_count(self):
        """Get current number of unique whales in database"""
        conn = self.get_database_connection()
        if not conn:
            return 0
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM whale_transactions")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            print(f"⚠️ Failed to get whale count: {e}")
            return 0
        finally:
            conn.close()
    
    def get_token_price(self, contract_address):
        """Get token price using CoinGecko Pro API"""
        try:
            url = f"{COINGECKO_PRO_BASE_URL}/simple/token_price/ethereum"
            headers = {'x-cg-pro-api-key': COINGECKO_PRO_API_KEY}
            params = {
                'contract_addresses': contract_address,
                'vs_currencies': 'usd'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get(contract_address.lower(), {}).get('usd', 0)
            
        except Exception as e:
            print(f"⚠️ Price fetch failed for {contract_address}: {e}")
        return 0
    
    def get_current_block(self):
        """Get current Ethereum block number"""
        try:
            url = f"https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}"
            response = requests.get(url, timeout=10)
            return int(response.json()['result'], 16)
        except Exception as e:
            print(f"⚠️ Failed to get current block: {e}")
            return 0
    
    def discover_new_whales(self):
        """Discover new whale wallets in the last 12 hours"""
        print(f"\n🔍 WHALE DISCOVERY SCAN #{self.scan_count + 1}")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        current_block = self.get_current_block()
        if current_block == 0:
            print("❌ Cannot get current block")
            return 0
        
        # Calculate 6-hour block range (since last FCB session)
        blocks_back = SCAN_HOURS_BACK * 300  # ~300 blocks per hour
        start_block = current_block - blocks_back
        
        print(f"📊 Scanning blocks {start_block:,} to {current_block:,}")
        print(f"🕐 Looking back {SCAN_HOURS_BACK} hours (since last FCB session)")
        print(f"🎯 Preparing whale intelligence for FCB session")
        
        new_whales_found = 0
        total_volume = 0
        
        # Scan each target token
        for token_symbol, token_data in target_tokens['ethereum'].items():
            print(f"\n🎯 Scanning {token_symbol} for new whales...")
            
            contract_address = token_data['contract']
            decimals = token_data['decimals']
            
            # Get current token price
            token_price = self.get_token_price(contract_address)
            if token_price == 0:
                print(f"⚠️ No price data for {token_symbol}, skipping...")
                continue
            
            print(f"💰 {token_symbol} price: ${token_price:.8f}")
            
            # Rate limiting for Etherscan (respect 2 calls/sec limit)
            time.sleep(0.6)
            
            # Get transfer events
            url = "https://api.etherscan.io/api"
            params = {
                'module': 'logs',
                'action': 'getLogs',
                'fromBlock': start_block,
                'toBlock': current_block,
                'address': contract_address,
                'topic0': '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                'apikey': ETHERSCAN_API_KEY
            }
            
            try:
                response = requests.get(url, params=params, timeout=30)
                data = response.json()
                
                if data.get('status') != '1':
                    error_msg = data.get('message', 'Unknown error')
                    print(f"⚠️ API error for {token_symbol}: {error_msg}")
                    
                    if 'rate limit' in error_msg.lower():
                        print("⏰ Rate limit hit - waiting 10 seconds...")
                        time.sleep(10)
                    continue
                
                transfers = data.get('result', [])
                print(f"📈 Found {len(transfers):,} {token_symbol} transfers")
                
                token_whales = 0
                token_volume = 0
                
                # Process transfers to find whales
                for log in transfers:
                    try:
                        # Decode transfer data
                        transfer_data = log.get('data', '0x')
                        if len(transfer_data) < 66:
                            continue
                        
                        # Extract amount
                        amount_hex = transfer_data[2:66]
                        amount_tokens = int(amount_hex, 16) / (10 ** decimals)
                        amount_usd = amount_tokens * token_price
                        
                        # Check whale threshold
                        if amount_usd >= WHALE_THRESHOLD:
                            topics = log.get('topics', [])
                            if len(topics) >= 3:
                                from_address = '0x' + topics[1][-40:]
                                to_address = '0x' + topics[2][-40:]
                                
                                # Determine whale address and activity type
                                whale_address = from_address
                                activity_type = 'transfer'
                                
                                # Check for DEX interactions
                                dex_addresses = {
                                    '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',  # Uniswap V2
                                    '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3
                                    '0x1111111254fb6c44bac0bed2854e76f90643097d',  # 1inch
                                    '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Protocol
                                }
                                
                                if from_address.lower() in [addr.lower() for addr in dex_addresses]:
                                    activity_type = 'buy'
                                    whale_address = to_address
                                elif to_address.lower() in [addr.lower() for addr in dex_addresses]:
                                    activity_type = 'sell'
                                    whale_address = from_address
                                
                                # Create whale transaction record
                                whale_transaction = {
                                    'transaction_id': log.get('transactionHash', ''),
                                    'wallet_address': whale_address,
                                    'blockchain': 'ethereum',
                                    'block_number': int(log.get('blockNumber', '0x0'), 16),
                                    'block_timestamp': datetime.now(),
                                    'from_address': from_address,
                                    'to_address': to_address,
                                    'coin_symbol': token_symbol,
                                    'activity_type': activity_type,
                                    'amount_tokens': Decimal(str(amount_tokens)),
                                    'amount_usd': Decimal(str(amount_usd)),
                                    'raw_transaction': log,
                                    'data_source': 'discovery_scanner'
                                }
                                
                                # Save to database
                                if self.save_whale_transaction(whale_transaction):
                                    token_whales += 1
                                    token_volume += amount_usd
                                    
                                    if token_whales <= 5:  # Show first 5 whales per token
                                        print(f"🐋 NEW WHALE: {whale_address[:10]}... ${amount_usd:,.2f} {token_symbol} {activity_type}")
                    
                    except Exception as e:
                        continue
                
                if token_whales > 0:
                    print(f"✅ {token_symbol}: {token_whales} new whales, ${token_volume:,.2f} volume")
                    new_whales_found += token_whales
                    total_volume += token_volume
                else:
                    print(f"😴 {token_symbol}: No new whale activity")
                
            except Exception as e:
                print(f"❌ Failed to scan {token_symbol}: {e}")
        
        return new_whales_found, total_volume
    
    def generate_discovery_report(self, new_whales, volume, next_session_time):
        """Generate discovery scan report with FCB session timing"""
        current_whale_count = self.get_current_whale_count()
        
        print(f"\n" + "=" * 80)
        print(f"📊 FCB PRE-SESSION WHALE DISCOVERY COMPLETE")
        print(f"=" * 80)
        print(f"🐋 New whales discovered: {new_whales}")
        print(f"💰 New volume analyzed: ${volume:,.2f}")
        print(f"📈 Total whales in database: {current_whale_count}")
        print(f"🎯 Next FCB session: {next_session_time.strftime('%H:%M UTC (%Y-%m-%d)')}")
        print(f"⏰ Time until session: {int((next_session_time - datetime.utcnow()).total_seconds() / 60)} minutes")
        
        self.total_whales_discovered += new_whales
        
        if new_whales > 0:
            print(f"🚀 SUCCESS! Fresh whale intelligence ready for FCB session")
        else:
            print(f"😴 No new whales since last session - database stable")
        
        # Calculate next discovery time
        next_discovery_time, next_next_session = self.get_next_fcb_session_time()
        print(f"🔍 Next whale discovery: {next_discovery_time.strftime('%H:%M UTC (%Y-%m-%d)')}")
    
    async def run_discovery_loop(self):
        """Main discovery loop - runs 30 minutes before each FCB session"""
        print("🚀 FCB WHALE DISCOVERY SCANNER - PRE-SESSION INTELLIGENCE")
        print("=" * 80)
        print(f"🕐 FCB Sessions: {', '.join([f'{h:02d}:00 UTC' for h in FCB_SESSION_HOURS])}")
        print(f"🔍 Discovery timing: 30 minutes before each session")
        print(f"🎯 Whale threshold: ${WHALE_THRESHOLD:,}+")
        print(f"📊 Lookback period: {SCAN_HOURS_BACK} hours per scan")
        print(f"🪙 Target tokens: {len(target_tokens['ethereum'])} tokens")
        
        while True:
            try:
                # Calculate next discovery time
                next_discovery_time, next_session_time = self.get_next_fcb_session_time()
                current_time = datetime.utcnow()
                
                # Calculate sleep time until next discovery
                sleep_seconds = (next_discovery_time - current_time).total_seconds()
                
                if sleep_seconds > 0:
                    print(f"\n⏰ Next discovery: {next_discovery_time.strftime('%H:%M:%S UTC (%Y-%m-%d)')}")
                    print(f"🎯 For FCB session: {next_session_time.strftime('%H:%M:%S UTC')}")
                    print(f"💤 Sleeping for {sleep_seconds/3600:.1f} hours...")
                    await asyncio.sleep(sleep_seconds)
                
                self.scan_count += 1
                
                print(f"\n🚨 FCB PRE-SESSION WHALE DISCOVERY #{self.scan_count}")
                print(f"🕐 Discovery time: {datetime.utcnow().strftime('%H:%M:%S UTC')}")
                print(f"🎯 FCB session starts: {next_session_time.strftime('%H:%M:%S UTC')} (in 30 minutes)")
                
                # Run whale discovery
                new_whales, volume = self.discover_new_whales()
                
                # Generate report with session timing
                self.generate_discovery_report(new_whales, volume, next_session_time)
                
            except KeyboardInterrupt:
                print("\n🛑 FCB Discovery scanner stopped by user")
                break
            except Exception as e:
                print(f"⚠️ Discovery error: {e}")
                print("🔄 Retrying in 30 minutes...")
                await asyncio.sleep(1800)  # Wait 30 minutes before retry

def main():
    """Main execution"""
    scanner = WhaleDiscoveryScanner()
    
    try:
        # Run the discovery loop
        asyncio.run(scanner.run_discovery_loop())
    except KeyboardInterrupt:
        print("\n👋 FCB Whale Discovery Scanner stopped")

if __name__ == "__main__":
    main()
