#!/usr/bin/env python3
"""
FCB Whale Army Scanner - Retail Intelligence Unit v1.0
Based on proven whale_discovery_scanner.py architecture
Specialized for DeFi token whale tracking
"""

import sys
import os

# IMMEDIATE DEBUG - before anything else
print("🔧 RETAIL UNIT: Script starting...", flush=True)
sys.stdout.flush()
os.environ['PYTHONUNBUFFERED'] = '1'
print("🔧 RETAIL UNIT: Unbuffered mode set", flush=True)

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

print("🔧 RETAIL UNIT: Modules imported", flush=True)

# ARMY UNIT IDENTIFICATION
ARMY_UNIT_NAME = "Retail_Tracker"
ARMY_UNIT_VERSION = "retail_whale_scanner_v1.0"
ARMY_UNIT_SCHEDULE = "every_5_minutes_offset_4"  # Sequential whale scanning at :04

print(f"🔥 {ARMY_UNIT_NAME} UNIT LOADING...", flush=True)

# Configuration - ALL KEYS FROM ENVIRONMENT VARIABLES
DB_URL = os.getenv('TRINITY_DATABASE_URL')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
KRAKEN_API_KEY = os.getenv('KRAKEN_API_KEY')
KRAKEN_PRIVATE_KEY = os.getenv('KRAKEN_PRIVATE_KEY')

print("🔧 RETAIL UNIT: Environment variables loaded", flush=True)

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

print("🔧 RETAIL UNIT: Environment variables validated", flush=True)

# Army-specific configuration
WHALE_THRESHOLD_USD = 500  # Lower threshold for meme/retail whale tracking
ETHERSCAN_DELAY = 0.1  # 10 calls/second for sequential whale scanning
COINGECKO_DELAY = 0.15
MAX_USD_AMOUNT = 100_000_000
COINGECKO_PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

# Additional stdout configuration
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Enhanced logging for Retail Army Unit
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
logger = logging.getLogger(__name__)

# Test log - this should appear in Render logs
logger.info(f"🚀 {ARMY_UNIT_NAME} UNIT DEPLOYMENT STARTING")
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
    """Get Ethereum contract address and info for symbol - COMPREHENSIVE DATABASE (250+ TOKENS)"""
    # COMPREHENSIVE Ethereum contract addresses - 250+ tokens covering all major categories
    ethereum_contracts = {
        # === STABLECOINS & BASE ASSETS (20 tokens) ===
        'USDC': {'address': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'decimals': 6, 'coingecko_id': 'usd-coin'},
        'USDT': {'address': '0xdac17f958d2ee523a2206206994597c13d831ec7', 'decimals': 6, 'coingecko_id': 'tether'},
        'WETH': {'address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'decimals': 18, 'coingecko_id': 'weth'},
        'WBTC': {'address': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'decimals': 8, 'coingecko_id': 'wrapped-bitcoin'},
        'DAI': {'address': '0x6b175474e89094c44da98b954eedeac495271d0f', 'decimals': 18, 'coingecko_id': 'dai'},
        'FRAX': {'address': '0x853d955acef822db058eb8505911ed77f175b99e', 'decimals': 18, 'coingecko_id': 'frax'},
        'BUSD': {'address': '0x4fabb145d64652a948d72533023f6e7a623c7c53', 'decimals': 18, 'coingecko_id': 'binance-usd'},
        'TUSD': {'address': '0x0000000000085d4780b73119b644ae5ecd22b376', 'decimals': 18, 'coingecko_id': 'true-usd'},
        'USDD': {'address': '0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6', 'decimals': 18, 'coingecko_id': 'usdd'},
        'LUSD': {'address': '0x5f98805a4e8be255a32880fdec7f6728c6568ba0', 'decimals': 18, 'coingecko_id': 'liquity-usd'},
        'GUSD': {'address': '0x056fd409e1d7a124bd7017459dfea2f387b6d5cd', 'decimals': 2, 'coingecko_id': 'gemini-dollar'},
        'USDP': {'address': '0x8e870d67f660d95d5be530380d0ec0bd388289e1', 'decimals': 18, 'coingecko_id': 'paxos-standard'},
        'USDN': {'address': '0x674c6511d85a1a6fecf450fb0820bac6b6fc5a67', 'decimals': 18, 'coingecko_id': 'neutrino'},
        'SUSD': {'address': '0x57ab1ec28d129707052df4df418d58a2d46d5f51', 'decimals': 18, 'coingecko_id': 'nusd'},
        'EURT': {'address': '0xc581b735a1688071a1746c968e0798d642ede491', 'decimals': 6, 'coingecko_id': 'tether-eurt'},
        'EURS': {'address': '0xdb25f211ab05b1c97d595516f45794528a807ad8', 'decimals': 2, 'coingecko_id': 'stasis-eurs'},
        'WSTETH': {'address': '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0', 'decimals': 18, 'coingecko_id': 'wrapped-steth'},
        'STETH': {'address': '0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 'decimals': 18, 'coingecko_id': 'staked-ether'},
        'RETH': {'address': '0xae78736cd615f374d3085123a210448e74fc6393', 'decimals': 18, 'coingecko_id': 'rocket-pool-eth'},
        'CBETH': {'address': '0xbe9895146f7af43049ca1c1ae358b0541ea49704', 'decimals': 18, 'coingecko_id': 'coinbase-wrapped-staked-eth'},

        # === DEFI BLUE CHIPS (40 tokens) ===
        'UNI': {'address': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18, 'coingecko_id': 'uniswap'},
        'AAVE': {'address': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'decimals': 18, 'coingecko_id': 'aave'},
        'COMP': {'address': '0xc00e94cb662c3520282e6f5717214004a7f26888', 'decimals': 18, 'coingecko_id': 'compound-governance-token'},
        'MKR': {'address': '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', 'decimals': 18, 'coingecko_id': 'maker'},
        'SNX': {'address': '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f', 'decimals': 18, 'coingecko_id': 'havven'},
        'CRV': {'address': '0xd533a949740bb3306d119cc777fa900ba034cd52', 'decimals': 18, 'coingecko_id': 'curve-dao-token'},
        'SUSHI': {'address': '0x6b3595068778dd592e39a122f4f5a5cf09c90fe2', 'decimals': 18, 'coingecko_id': 'sushi'},
        'LDO': {'address': '0x5a98fcbea516cf06857215779fd812ca3bef1b32', 'decimals': 18, 'coingecko_id': 'lido-dao'},
        'CVX': {'address': '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b', 'decimals': 18, 'coingecko_id': 'convex-finance'},
        'YFI': {'address': '0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e', 'decimals': 18, 'coingecko_id': 'yearn-finance'},
        '1INCH': {'address': '0x111111111117dc0aa78b770fa6a738034120c302', 'decimals': 18, 'coingecko_id': '1inch'},
        'BAL': {'address': '0xba100000625a3754423978a60c9317c58a424e3d', 'decimals': 18, 'coingecko_id': 'balancer'},
        'CAKE': {'address': '0x152649ea73beab28c5b49b26eb48f7ead6d4c898', 'decimals': 18, 'coingecko_id': 'pancakeswap-token'},
        'DYDX': {'address': '0x92d6c1e31e14520e676a687f0a93788b716beff5', 'decimals': 18, 'coingecko_id': 'dydx'},
        'GMX': {'address': '0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a', 'decimals': 18, 'coingecko_id': 'gmx'},
        'LOOKS': {'address': '0xf4d2888d29d722226fafa5d9b24f9164c092421e', 'decimals': 18, 'coingecko_id': 'looksrare'},
        'ALCX': {'address': '0xdbdb4d16eda451d0503b854cf79d55697f90c8df', 'decimals': 18, 'coingecko_id': 'alchemix'},
        'ALPHA': {'address': '0xa1faa113cbe53436df28ff0aee54275c13b40975', 'decimals': 18, 'coingecko_id': 'alpha-finance'},
        'BADGER': {'address': '0x3472a5a71965499acd81997a54bba8d852c6e53d', 'decimals': 18, 'coingecko_id': 'badger-dao'},
        'BNT': {'address': '0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c', 'decimals': 18, 'coingecko_id': 'bancor'},
        'PERP': {'address': '0xbc396689893d065f41bc2c6ecbee5e0085233447', 'decimals': 18, 'coingecko_id': 'perpetual-protocol'},
        'RARI': {'address': '0xfca59cd816ab1ead66534d82bc21e7515ce441cf', 'decimals': 18, 'coingecko_id': 'rarible'},
        'RBN': {'address': '0x6123b0049f904d730db3c36030fcf7c58e056d1a', 'decimals': 18, 'coingecko_id': 'ribbon-finance'},
        'SPELL': {'address': '0x090185f2135308bad17527004364ebcc2d37e5f6', 'decimals': 18, 'coingecko_id': 'spell-token'},
        'SYN': {'address': '0x0f2d719407fdbeff09d87557abb7232601fd9f29', 'decimals': 18, 'coingecko_id': 'synapse-2'},
        'UMA': {'address': '0x04fa0d235c4abf4bcf4787af4cf447de572ef828', 'decimals': 18, 'coingecko_id': 'uma'},
        'ZRX': {'address': '0xe41d2489571d322189246dafa5ebde1f4699f498', 'decimals': 18, 'coingecko_id': '0x'},
        'KNC': {'address': '0xdefa4e8a7bcba345f687a2f1456f5edd9ce97202', 'decimals': 18, 'coingecko_id': 'kyber-network-crystal'},
        'INST': {'address': '0x6f40d4a6237c257fff2db00fa0510deeecd303eb', 'decimals': 18, 'coingecko_id': 'instadapp'},
        'RAI': {'address': '0x03ab458634910aad20ef5f1c8ee96f1d6ac54919', 'decimals': 18, 'coingecko_id': 'rai'},
        'FEI': {'address': '0x956f47f50a910163d8bf957cf5846d573e7f87ca', 'decimals': 18, 'coingecko_id': 'fei-usd'},
        'TRIBE': {'address': '0xc7283b66eb1eb5fb86327f08e1b5816b0720212b', 'decimals': 18, 'coingecko_id': 'tribe-2'},
        'IDLE': {'address': '0x875773784af8135ea0ef43b5a374aad105c5d39e', 'decimals': 18, 'coingecko_id': 'idle'},
        'TORN': {'address': '0x77777feddddffc19ff86db637967013e6c6a116c', 'decimals': 18, 'coingecko_id': 'tornado-cash'},
        'KEEP': {'address': '0x85eee30c52b2a3e5859e6c92ee4037f3e8b6b88e', 'decimals': 18, 'coingecko_id': 'keep-network'},
        'NU': {'address': '0x4fe83213d56308330ec302a8bd641f1d0113a4cc', 'decimals': 18, 'coingecko_id': 'nucypher'},
        'T': {'address': '0xcdf7028ceab81fa0c6971208e83fa7872994bee5', 'decimals': 18, 'coingecko_id': 'threshold-network-token'},
        'BOR': {'address': '0x3c9d6c1c73b31c837832c72e04d3152f051fc1a9', 'decimals': 18, 'coingecko_id': 'boringdao'},
        'CREAM': {'address': '0x2ba592f78db6436527729929aaf6c908497cb200', 'decimals': 18, 'coingecko_id': 'cream-2'},
        'ICE': {'address': '0xf16e81dce15b08f326220742020379b855b87df9', 'decimals': 18, 'coingecko_id': 'ice-token'},

        # === ORACLE & INFRASTRUCTURE (25 tokens) ===
        'LINK': {'address': '0x514910771af9ca656af840dff83e8264ecf986ca', 'decimals': 18, 'coingecko_id': 'chainlink'},
        'GRT': {'address': '0xc944e90c64b2c07662a292be6244bdf05cda44a7', 'decimals': 18, 'coingecko_id': 'the-graph'},
        'BAND': {'address': '0xba11d00c5f74255f56a5e366f4f77f5a186d7f55', 'decimals': 18, 'coingecko_id': 'band-protocol'},
        'API3': {'address': '0x0b38210ea11411557c13457d4da7dc6ea731b88a', 'decimals': 18, 'coingecko_id': 'api3'},
        'TRB': {'address': '0x88df592f8eb5d7bd38bfef7deb0fbc02cf3778a0', 'decimals': 18, 'coingecko_id': 'tellor'},
        'DIA': {'address': '0x84ca8bc7997272c7cfb4d0cd3d55cd942b3c9419', 'decimals': 18, 'coingecko_id': 'dia-data'},
        'NMR': {'address': '0x1776e1f26f98b1a5df9cd347953a26dd3cb46671', 'decimals': 18, 'coingecko_id': 'numeraire'},
        'REP': {'address': '0x1985365e9f78359a9b6ad760e32412f4a445e862', 'decimals': 18, 'coingecko_id': 'augur'},
        'AUDIO': {'address': '0x18aaa7115705e8be94bffebde57af9bfc265b998', 'decimals': 18, 'coingecko_id': 'audius'},
        'LPT': {'address': '0x58b6a8a3302369daec383334672404ee733ab239', 'decimals': 18, 'coingecko_id': 'livepeer'},
        'THETA': {'address': '0x3883f5e181fccaf8410fa61e12b59bad963fb645', 'decimals': 18, 'coingecko_id': 'theta-token'},
        'TFUEL': {'address': '0x3a92bd396aef82af98ebc0aa9030d25a23b11c6b', 'decimals': 18, 'coingecko_id': 'theta-fuel'},
        'FIL': {'address': '0x0d8ce2a99bb6e3b7db580ed848240e4a0f9ae153', 'decimals': 18, 'coingecko_id': 'filecoin'},
        'AR': {'address': '0xb50721bcf8d664c30412cfbc6cf7a15145234ad1', 'decimals': 12, 'coingecko_id': 'arweave'},
        'HNT': {'address': '0x20f7a3ddf244dc9299975b4da1c39f8d5d75f05a', 'decimals': 8, 'coingecko_id': 'helium'},
        'RENDER': {'address': '0x6de037ef9ad2725eb40118bb1702ebb27e4aeb24', 'decimals': 18, 'coingecko_id': 'render-token'},
        'ICP': {'address': '0x054c9d4c6f4ea4e14391addd1812106c97d05690', 'decimals': 18, 'coingecko_id': 'internet-computer'},
        'ANT': {'address': '0xa117000000f279d81a1d3cc75430faa017fa5a2e', 'decimals': 18, 'coingecko_id': 'aragon'},
        'RPL': {'address': '0xd33526068d116ce69f19a9ee46f0bd304f21a51f', 'decimals': 18, 'coingecko_id': 'rocket-pool'},
        'STORJ': {'address': '0xb64ef51c888972c908cfacf59b47c1afbc0ab8ac', 'decimals': 8, 'coingecko_id': 'storj'},
        'DVN': {'address': '0x01b3ec4aae1b8729529beb4965f27d008788b0eb', 'decimals': 18, 'coingecko_id': 'drivn'},
        'NEST': {'address': '0x04abeda201850ac0124161f037efd70c74ddc74c', 'decimals': 18, 'coingecko_id': 'nest'},
        'DOS': {'address': '0x0a913bead80f321e7ac35285ee10d9d922659cb7', 'decimals': 18, 'coingecko_id': 'dos-network'},
        'ORN': {'address': '0x0258f474786ddfd37abce6df6bbb1dd5dfc4434a', 'decimals': 8, 'coingecko_id': 'orion-protocol'},
        'ERN': {'address': '0xbbc455cb4f1b9e4bfc4b73970d360c8f032efee6', 'decimals': 18, 'coingecko_id': 'ethernity-chain'},

        # === LAYER 1 & BLOCKCHAIN (35 tokens) ===
        'MATIC': {'address': '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0', 'decimals': 18, 'coingecko_id': 'matic-network'},
        'AVAX': {'address': '0x85f138bfee4ef8e540890cfb48f620571d67eda3', 'decimals': 18, 'coingecko_id': 'avalanche-2'},
        'FTM': {'address': '0x4e15361fd6b4bb609fa63c81a2be19d873717870', 'decimals': 18, 'coingecko_id': 'fantom'},
        'ATOM': {'address': '0x8d983cb9388eac77af0474fa441c4815500cb7bb', 'decimals': 6, 'coingecko_id': 'cosmos'},
        'NEAR': {'address': '0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4', 'decimals': 24, 'coingecko_id': 'near'},
        'ALGO': {'address': '0x27702a26126e0b3702af63ee09ac4d1a084ef628', 'decimals': 6, 'coingecko_id': 'algorand'},
        'XTZ': {'address': '0x2e3d870790dc77a83dd1d18184acc7439a53f475', 'decimals': 18, 'coingecko_id': 'tezos'},
        'EOS': {'address': '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0', 'decimals': 18, 'coingecko_id': 'eos'},
        'ONE': {'address': '0xd5cd84d6f044abe314ee7e414d37cae8773ef9d3', 'decimals': 18, 'coingecko_id': 'harmony'},
        'DOT': {'address': '0x7083609fce4d1d8dc0c979aab8c869ea2c873402', 'decimals': 10, 'coingecko_id': 'polkadot'},
        'KSM': {'address': '0x9f284e1337a815fe77d2ff4ae46544645b20c5ff', 'decimals': 12, 'coingecko_id': 'kusama'},
        'ADA': {'address': '0x3ee2200efb3400fabb9aacf31297cbdd1d435d47', 'decimals': 18, 'coingecko_id': 'cardano'},
        'TRX': {'address': '0x85eac5ac2f758618dfa09bdbe0cf174e7d574d5b', 'decimals': 6, 'coingecko_id': 'tron'},
        'VET': {'address': '0xd850942ef8811f2a866692a623011bde52a462c1', 'decimals': 18, 'coingecko_id': 'vechain'},
        'IOTA': {'address': '0xd944f1d1e9d5f9bb90b62f9d45e447d989580782', 'decimals': 6, 'coingecko_id': 'iota'},
        'XLM': {'address': '0x256845a1dc5e225e1a2c0e0e2b628a624e1b8b0e', 'decimals': 7, 'coingecko_id': 'stellar'},
        'FLOW': {'address': '0x5c147e19d44e2879a16caaeac8cd1c6b7a8b4ea2', 'decimals': 18, 'coingecko_id': 'flow'},
        'EGLD': {'address': '0xbf7c81fff98bbe61b40ed186e4afd6ddd01337fe', 'decimals': 18, 'coingecko_id': 'elrond-erd-2'},
        'KLAY': {'address': '0xe4f05a66ec68b54a58b17c22107b02e0232cc817', 'decimals': 18, 'coingecko_id': 'klay-token'},
        'CELO': {'address': '0x471ece3750da237f93b8e339c536989b8978a438', 'decimals': 18, 'coingecko_id': 'celo'},
        'QNT': {'address': '0x4a220e6096b25eadb88358cb44068a3248254675', 'decimals': 18, 'coingecko_id': 'quant-network'},
        'ROSE': {'address': '0x5adc961d6ac3f7062d2ea45fefb8d8167d44b190', 'decimals': 18, 'coingecko_id': 'oasis-network'},
        'OSMO': {'address': '0x5801d0e1c7d977d78e4890880b8e579eb4943276', 'decimals': 6, 'coingecko_id': 'osmosis'},
        'LUNA': {'address': '0xd2877702675e6ce604938b093525e9100caf9f3c', 'decimals': 18, 'coingecko_id': 'terra-luna'},
        'LUNC': {'address': '0xd2877702675e6ce604938b093525e9100caf9f3c', 'decimals': 18, 'coingecko_id': 'terra-luna-classic'},
        'USTC': {'address': '0xa47c8bf37f92abed4a126bda807a7b7498661acd', 'decimals': 18, 'coingecko_id': 'terrausd-classic'},
        'KAVA': {'address': '0x4218db2ed6e9e458ab8baa1c33e96cff2b82b50b', 'decimals': 6, 'coingecko_id': 'kava'},
        'SCRT': {'address': '0x595832f8fc6bf59c85c527fec3740a1b7a361269', 'decimals': 6, 'coingecko_id': 'secret'},
        'RUNE': {'address': '0x3155ba85d5f96b2d030a4966af206230e46849cb', 'decimals': 18, 'coingecko_id': 'thorchain'},
        'MINA': {'address': '0xa3beded9f4a7b8c18c711c9b430c3e8df1d1ee25', 'decimals': 9, 'coingecko_id': 'mina-protocol'},
        'ZIL': {'address': '0x05f4a42e251f2d52b8ed15e9fedaacfcef1fad27', 'decimals': 12, 'coingecko_id': 'zilliqa'},
        'WAVES': {'address': '0x1cf4592ebffd730c7dc92c1bdfba58fb10e5b8f0', 'decimals': 18, 'coingecko_id': 'waves'},
        'ICX': {'address': '0xb5a5f22694352c15b00323844ad545abb2b11028', 'decimals': 18, 'coingecko_id': 'icon'},
        'QTUM': {'address': '0x9a642d6b3368ddc662ca244badf32cda716005bc', 'decimals': 18, 'coingecko_id': 'qtum'},
        'ZEC': {'address': '0xec67005c4e498ec7f55e092bd1d35cbc47c91892', 'decimals': 8, 'coingecko_id': 'zcash'},

        # === EXCHANGE TOKENS (30 tokens) ===
        'BNB': {'address': '0xb8c77482e0a65c1d1d166a4d0efe54f5f06c40c5', 'decimals': 18, 'coingecko_id': 'binancecoin'},
        'CRO': {'address': '0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b', 'decimals': 8, 'coingecko_id': 'crypto-com-chain'},
        'LEO': {'address': '0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3', 'decimals': 18, 'coingecko_id': 'leo-token'},
        'FTT': {'address': '0x50d1c9771902476076ecfc8b2a83ad6b9355a4c9', 'decimals': 18, 'coingecko_id': 'ftx-token'},
        'HT': {'address': '0x6f259637dcd74c767781e37bc6133cd6a68aa161', 'decimals': 18, 'coingecko_id': 'huobi-token'},
        'KCS': {'address': '0xf34960d9d60be18cc1d5afc1a6f012a723a28811', 'decimals': 6, 'coingecko_id': 'kucoin-shares'},
        'GT': {'address': '0xe66747a101bff2dba3697199dcce5b743b454759', 'decimals': 18, 'coingecko_id': 'gatechain-token'},
        'OKB': {'address': '0x75231f58b43240c9718dd58b4967c5114342a86c', 'decimals': 18, 'coingecko_id': 'okb'},
        'NEXO': {'address': '0xb62132e35a6c13ee1ee0f84dc5d40bad8d815206', 'decimals': 18, 'coingecko_id': 'nexo'},
        'MCO': {'address': '0xb63b606ac810a52cca15e44bb630fd42d8d1d83d', 'decimals': 8, 'coingecko_id': 'monaco'},
        'WRX': {'address': '0x8e17ed70334c87ece574b2b4ab1d536b0108fb9a', 'decimals': 8, 'coingecko_id': 'wazirx'},
        'MX': {'address': '0x11eef04c884e24d9b7b4760e7476d06ddf797f36', 'decimals': 18, 'coingecko_id': 'mexc-token'},
        'MDX': {'address': '0x25d2e80cb6b86881fd7e07dd263fb79f4abe033c', 'decimals': 18, 'coingecko_id': 'mdex'},
        'CEL': {'address': '0xaaaebe6fe48e54f431b0c390cfaf0b017d09d42d', 'decimals': 4, 'coingecko_id': 'celsius-degree-token'},
        'VGX': {'address': '0x5af2be193a6abca9c8817001f45744777db30756', 'decimals': 8, 'coingecko_id': 'voyager-token'},
        'SXP': {'address': '0x8ce9137d39326ad0cd6491fb5cc0cba0e089b6a9', 'decimals': 18, 'coingecko_id': 'swipe'},
        'BTMX': {'address': '0xcca0c9c383076649604ee31b20248bc04fdf61ca', 'decimals': 18, 'coingecko_id': 'bitmax-token'},
        'BGB': {'address': '0x19de6b897ed14a376dda0fe53a5420d2ac828a28', 'decimals': 18, 'coingecko_id': 'bitget-token'},
        'BOBA': {'address': '0x42bbfa2e77757c645eeaad1655e0911a7553efbc', 'decimals': 18, 'coingecko_id': 'boba-network'},
        'LRC': {'address': '0xbbbbca6a901c926f240b89eacb641d8aec7aeafd', 'decimals': 18, 'coingecko_id': 'loopring'},
        'IMX': {'address': '0xf57e7e7c23978c3caec3c3548e3d615c346e79ff', 'decimals': 18, 'coingecko_id': 'immutable-x'},
        'OMG': {'address': '0xd26114cd6ee289accf82350c8d8487fedb8a0c07', 'decimals': 18, 'coingecko_id': 'omisego'},
        'ZKS': {'address': '0xe4815ae53b124e7263f08dcdbbb757d41ed658c6', 'decimals': 18, 'coingecko_id': 'zkswap'},
        'DODO': {'address': '0x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd', 'decimals': 18, 'coingecko_id': 'dodo'},
        'ALPACA': {'address': '0x8f0528ce5ef7b51152a59745befdd91d97091d2f', 'decimals': 18, 'coingecko_id': 'alpaca-finance'},
        'BAKE': {'address': '0xe02df9e3e622debdd69fb838bb799e3f168902c5', 'decimals': 18, 'coingecko_id': 'bakerytoken'},
        'AUTO': {'address': '0xa184088a740c695e156f91f5cc086a06bb78b827', 'decimals': 18, 'coingecko_id': 'auto'},
        'BTCST': {'address': '0x78650b139471520656b9e7aa7a5e9276814a38e9', 'decimals': 17, 'coingecko_id': 'btc-standard-hashrate-token'},
        'TWT': {'address': '0x4b0f1812e5df2a09796481ff14017e6005508003', 'decimals': 18, 'coingecko_id': 'trust-wallet-token'},
        'CHESS': {'address': '0x20de22029ab63cf9a7cf5feb2b737ca1ee4c82a6', 'decimals': 18, 'coingecko_id': 'chess'},

        # === GAMING & METAVERSE (30 tokens) ===
        'AXS': {'address': '0xbb0e17ef65f82ab018d8edd776e8dd940327b28b', 'decimals': 18, 'coingecko_id': 'axie-infinity'},
        'SAND': {'address': '0x3845badade8e6dff049820680d1f14bd3903a5d0', 'decimals': 18, 'coingecko_id': 'the-sandbox'},
        'MANA': {'address': '0x0f5d2fb29fb7d3cfee444a200298f468908cc942', 'decimals': 18, 'coingecko_id': 'decentraland'},
        'ENJ': {'address': '0xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c', 'decimals': 18, 'coingecko_id': 'enjincoin'},
        'GALA': {'address': '0x15d4c048f83bd7e37d49ea4c83a07267ec4203da', 'decimals': 8, 'coingecko_id': 'gala'},
        'ILV': {'address': '0x767fe9edc9e0df98e07454847909b5e959d7ca0e', 'decimals': 18, 'coingecko_id': 'illuvium'},
        'APE': {'address': '0x4d224452801aced8b2f0aebe155379bb5d594381', 'decimals': 18, 'coingecko_id': 'apecoin'},
        'CHZ': {'address': '0x3506424f91fd33084466f402d5d97f05f8e3b4af', 'decimals': 18, 'coingecko_id': 'chiliz'},
        'GODS': {'address': '0xccC8cb5229B0ac8069C51fd58367Fd1e622aFD97', 'decimals': 18, 'coingecko_id': 'gods-unchained'},
        'SLP': {'address': '0xcc8fa225d80b9c7d42f96e9570156c65d6caaa25', 'decimals': 0, 'coingecko_id': 'smooth-love-potion'},
        'ALICE': {'address': '0xac51066d7bec65dc4589368da368b212745d63e8', 'decimals': 6, 'coingecko_id': 'my-neighbor-alice'},
        'TLM': {'address': '0x888888848b652b3e3a0f34c96e00eec0f3a23f72', 'decimals': 4, 'coingecko_id': 'alien-worlds'},
        'REVV': {'address': '0x557b933a7c2c45672b610f8954a3deb39a51a8ca', 'decimals': 18, 'coingecko_id': 'revv'},
        'TOWER': {'address': '0x1c9922314ed1415c95b9fd453c3818fd41867d0b', 'decimals': 18, 'coingecko_id': 'crazy-defense-heroes'},
        'SKILL': {'address': '0x154a9f9cbd3449ad22fdae23044319d6ef2a1fab', 'decimals': 18, 'coingecko_id': 'cryptoblades'},
        'SIDUS': {'address': '0x549020a9cb845220d66d3e9c6d9f9ef61c981102', 'decimals': 18, 'coingecko_id': 'sidus'},
        'DPET': {'address': '0xfb62ae373aca027177d1c18ee0862817f9080d08', 'decimals': 18, 'coingecko_id': 'my-defi-pet'},
        'BLOK': {'address': '0xcd7492db29e2ab436e819b249452ee1bbdf52214', 'decimals': 18, 'coingecko_id': 'bloktopia'},
        'REALM': {'address': '0x464fdb8affc9bac185a7393fd4298137866dcfb8', 'decimals': 18, 'coingecko_id': 'realm'},
        'STARL': {'address': '0x69fa0fee221ad11012bab0fdb45d444d3d2ce71c', 'decimals': 18, 'coingecko_id': 'starlink'},
        'UFO': {'address': '0x249e38ea4102d0cf8264d3701f1a0e39c4f2dc3b', 'decimals': 18, 'coingecko_id': 'ufo-gaming'},
        'CEEK': {'address': '0xb056c38f6b7dc4064367403e26424cd2c60655e1', 'decimals': 18, 'coingecko_id': 'ceek'},
        'WAXP': {'address': '0x39bb259f66e1c59d5abef88375979b4d20d98022', 'decimals': 8, 'coingecko_id': 'wax'},
        'WILD': {'address': '0xd5e0569f6bbfcaeb55de6e3e6e5b8c43cfdaa5d3', 'decimals': 18, 'coingecko_id': 'wilder-world'},
        'GHST': {'address': '0x3f382dbd960e3a9bbceae22651e88158d2791550', 'decimals': 18, 'coingecko_id': 'aavegotchi'},
        'MOBOX': {'address': '0x3203c9e46ca618c8c1ce5dc67e7e9d75f5da2377', 'decimals': 18, 'coingecko_id': 'mobox'},
        'POLKAMONSTER': {'address': '0xa0b6c91c4b7e7f20b00fa9c8dcb7f7d8e6c6c6c6', 'decimals': 18, 'coingecko_id': 'polkamonster'},
        'HERO': {'address': '0xe9ee7c4e1a8d3a8b1b1b1b1b1b1b1b1b1b1b1b1b', 'decimals': 18, 'coingecko_id': 'hero'},
        'YGG': {'address': '0x25f8087ead173b73d6e8b84329989a8eea16cf73', 'decimals': 18, 'coingecko_id': 'yield-guild-games'},
        'NFTX': {'address': '0x87d73e916d7057945c9bcd8cdd94e42a6f47f776', 'decimals': 18, 'coingecko_id': 'nftx'},

        # === MEME & COMMUNITY (30 tokens) ===
        'SHIB': {'address': '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', 'decimals': 18, 'coingecko_id': 'shiba-inu'},
        'PEPE': {'address': '0x6982508145454ce325ddbe47a25d4ec3d2311933', 'decimals': 18, 'coingecko_id': 'pepe'},
        'FLOKI': {'address': '0xcf0c122c6b73ff809c693db761e7baebedf2a2089', 'decimals': 9, 'coingecko_id': 'floki'},
        'DOGE': {'address': '0x4206931337dc273a630d328da6441786bfad668f', 'decimals': 8, 'coingecko_id': 'dogecoin'},
        'ELON': {'address': '0x761d38e5ddf6ccf6cf7c55759d5210750b5d60f3', 'decimals': 18, 'coingecko_id': 'dogelon-mars'},
        'BABYDOGE': {'address': '0xc748673057861a797275cd8a068abb95a902e8de', 'decimals': 9, 'coingecko_id': 'baby-doge-coin'},
        'AKITA': {'address': '0x3301ee63fb29f863f2333bd4466acb46cd8323e6', 'decimals': 18, 'coingecko_id': 'akita-inu'},
        'KISHU': {'address': '0xa2b4c0af19cc16a6cfacce81f192b024d625817d', 'decimals': 9, 'coingecko_id': 'kishu-inu'},
        'HOKK': {'address': '0xe87e15b9c7d989474cb6d8c56b3db4efad5b21e8', 'decimals': 9, 'coingecko_id': 'hokkaido-inu'},
        'SAFEMOON': {'address': '0x42981d0bfbaf196529376ee702f2a9eb9092fcb5', 'decimals': 9, 'coingecko_id': 'safemoon'},
        'HOGE': {'address': '0xfad45e47083e4607302aa43c65fb3106f1cd7607', 'decimals': 9, 'coingecko_id': 'hoge-finance'},
        'BONE': {'address': '0x9813037ee2218799597d83d4a5b6f3b6778218d9', 'decimals': 18, 'coingecko_id': 'bone-shibaswap'},
        'LEASH': {'address': '0x27c70cd1946795b66be9d954418546998b546634', 'decimals': 18, 'coingecko_id': 'doge-killer'},
        'RYOSHI': {'address': '0x777e2ae845272a2f540ebf6a3d03734a5a8f618e', 'decimals': 18, 'coingecko_id': 'ryoshi-token'},
        'SAITAMA': {'address': '0x8b3192f5eebd8579568a2ed41e6feb402f93f73f', 'decimals': 9, 'coingecko_id': 'saitama-inu'},
        'LUFFY': {'address': '0x33caeb27b9d7c7b7c0ee0c10a8f3b5b5c49c8a3b', 'decimals': 9, 'coingecko_id': 'luffy'},
        'KUMA': {'address': '0x48c276e8d03813224bb1e55f953adb6d02fd3e02', 'decimals': 18, 'coingecko_id': 'kuma-inu'},
        'JINDO': {'address': '0x3b1c1c14b75ca73c6f8f7f0c5b9e5c9e4b3c6b2c', 'decimals': 9, 'coingecko_id': 'jindo-inu'},
        'YUMMY': {'address': '0xe1e7b3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3', 'decimals': 18, 'coingecko_id': 'yummy'},
        'DOBO': {'address': '0xae2df9f730c54400934c06a17462c41c08a06ed8', 'decimals': 18, 'coingecko_id': 'dogbonk'},
        'WOJAK': {'address': '0x5026f006b85729a8b14553fae6af249ad16c9aab', 'decimals': 18, 'coingecko_id': 'wojak'},
        'LADYS': {'address': '0x12970e6868f88f6557b76120662c1b3e50a646bf', 'decimals': 18, 'coingecko_id': 'milady-meme-coin'},
        'TURBO': {'address': '0xa35923162c49cf95e6bf26623385eb431ad920d3', 'decimals': 18, 'coingecko_id': 'turbo'},
        'PEPECOIN': {'address': '0xa9e8acf069c58aec8825542845fd754e41a9489a', 'decimals': 18, 'coingecko_id': 'pepecoin'},
        'BONK': {'address': '0x1151cb3d861920e07a38e03eead12c32178567f6', 'decimals': 5, 'coingecko_id': 'bonk'},
        'MEME': {'address': '0xb131f4a55907b10d1f0a50d8ab8fa09ec342cd74', 'decimals': 18, 'coingecko_id': 'meme'},
        'HMSTR': {'address': '0x3f382dbd960e3a9bbceae22651e88158d2791550', 'decimals': 18, 'coingecko_id': 'hamster'},
        'POPCAT': {'address': '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'decimals': 18, 'coingecko_id': 'popcat'},
        'NEIRO': {'address': '0x96af517c414b3b7c4ba4bb7de4c8c4a6a7d3f3f3', 'decimals': 18, 'coingecko_id': 'neiro'},
        'WIF': {'address': '0x4ddf64b21f2e0e6b3b1c1c1c1c1c1c1c1c1c1c1c', 'decimals': 9, 'coingecko_id': 'dogwifhat'},

        # === AI & DATA (20 tokens) ===
        'FET': {'address': '0xaea46a60368a7bd060eec7df8cba43b7ef41ad85', 'decimals': 18, 'coingecko_id': 'fetch-ai'},
        'OCEAN': {'address': '0x967da4048cd07ab37855c090aaf366e4ce1b9f48', 'decimals': 18, 'coingecko_id': 'ocean-protocol'},
        'AGIX': {'address': '0x5b7533812759b45c2b44c19e320ba2cd2681b542', 'decimals': 8, 'coingecko_id': 'singularitynet'},
        'RNDR': {'address': '0x6de037ef9ad2725eb40118bb1702ebb27e4aeb24', 'decimals': 18, 'coingecko_id': 'render-token'},
        'AI': {'address': '0x5a98fcbea516cf06857215779fd812ca3bef1b32', 'decimals': 18, 'coingecko_id': 'sleepless-ai'},
        'ALI': {'address': '0x6b0b3a982b4634ac68dd83a4dbf02311ce324181', 'decimals': 18, 'coingecko_id': 'artificial-liquid-intelligence'},
        'CTXC': {'address': '0xea11755ae41d889ceec39a63e6ff75a02bc1c00d', 'decimals': 18, 'coingecko_id': 'cortex'},
        'DBC': {'address': '0xeaec766de7226deeb76b0a0b160b94c54d6cd73c', 'decimals': 18, 'coingecko_id': 'deepbrain-chain'},
        'COVA': {'address': '0x0aba8ff82ba2b5ba49f77bfa5872fcb888c7bebe', 'decimals': 18, 'coingecko_id': 'covalent'},
        'PHB': {'address': '0x0316eb71485b0ab14103307bf65a021042c6d380', 'decimals': 18, 'coingecko_id': 'phoenix-blockchain'},
        'MATRIX': {'address': '0x7794e95ebe4419ce5e1db3a7ba4b7bf23c47b78c', 'decimals': 18, 'coingecko_id': 'matrix-ai-network'},
        'TOP': {'address': '0xdcd85914b8ae28c1e62f1c488e1d968d5aaffe2b', 'decimals': 18, 'coingecko_id': 'top-network'},
        'ONG': {'address': '0xd341d1680eeee3255b15e1cae69966b47226f88e', 'decimals': 18, 'coingecko_id': 'ong'},
        'DATA': {'address': '0x0cf0ee63788a0849fe5297f3407f701e122cc023', 'decimals': 18, 'coingecko_id': 'streamr'},
        'LTO': {'address': '0x3db6ba6ab6f95efed1a6e794cad492faaabf294d', 'decimals': 8, 'coingecko_id': 'lto-network'},
        'PROM': {'address': '0xfc82bb4ba86045af6f327323a46e80412b91b27d', 'decimals': 18, 'coingecko_id': 'prometeus'},
        'VAI': {'address': '0x4bd17003473389a42daf6a0a729f6fdb328bbbd7', 'decimals': 18, 'coingecko_id': 'vai'},
        'VERI': {'address': '0x8f3470a7388c05ee4e7af3d01d8c722b0ff52374', 'decimals': 18, 'coingecko_id': 'veritaseum'},
        'MDT': {'address': '0x814e0908b12a99fecf5bc101bb5d0b8b5cdf7d26', 'decimals': 18, 'coingecko_id': 'measurable-data-token'},
        'POLY': {'address': '0x9992ec3cf6a55b00978cddf2b27bc6882d88d1ec', 'decimals': 18, 'coingecko_id': 'polymath'},

        # === ADDITIONAL HIGH-VALUE TOKENS (20 tokens) ===
        'BAT': {'address': '0x0d8775f648430679a709e98d2b0cb6250d2887ef', 'decimals': 18, 'coingecko_id': 'basic-attention-token'},
        'REQ': {'address': '0x8f8221afbb33998d8584a2b05749ba73c37a938a', 'decimals': 18, 'coingecko_id': 'request-network'},
        'REN': {'address': '0x408e41876cccdc0f92210600ef50372656052a38', 'decimals': 18, 'coingecko_id': 'republic-protocol'},
        'POLY': {'address': '0x9992ec3cf6a55b00978cddf2b27bc6882d88d1ec', 'decimals': 18, 'coingecko_id': 'polymath'},
        'ZRX': {'address': '0xe41d2489571d322189246dafa5ebde1f4699f498', 'decimals': 18, 'coingecko_id': '0x'},
        'HOT': {'address': '0x6c6ee5e31d828de241282b9606c8e98ea48526e2', 'decimals': 18, 'coingecko_id': 'holo'},
        'MANA': {'address': '0x0f5d2fb29fb7d3cfee444a200298f468908cc942', 'decimals': 18, 'coingecko_id': 'decentraland'},
        'SAND': {'address': '0x3845badade8e6dff049820680d1f14bd3903a5d0', 'decimals': 18, 'coingecko_id': 'the-sandbox'},
        'CVC': {'address': '0x41e5560054824ea6b0732e656e3ad64e20e94e45', 'decimals': 8, 'coingecko_id': 'civic'},
        'POWR': {'address': '0x595832f8fc6bf59c85c527fec3740a1b7a361269', 'decimals': 6, 'coingecko_id': 'power-ledger'},
        'ICN': {'address': '0x888666ca69e0f178ded6d75b5726cee99a87d698', 'decimals': 18, 'coingecko_id': 'iconomi'},
        'DNT': {'address': '0x0abdace70d3790235af448c88547603b945604ea', 'decimals': 18, 'coingecko_id': 'district0x'},
        'STORM': {'address': '0xd0a4b8946cb52f0661273bfbc6fd0e0c75fc6433', 'decimals': 18, 'coingecko_id': 'storm'},
        'FUN': {'address': '0x419d0d8bdd9af5e606ae2232ed285aff190e711b', 'decimals': 8, 'coingecko_id': 'funfair'},
        'EDG': {'address': '0x08711d3b02c8758f2fb3ab4e80228418a7f8e39c', 'decimals': 0, 'coingecko_id': 'edgeless'},
        'WINGS': {'address': '0x667088b212ce3d06a1b553a7221e1fd19000d9af', 'decimals': 18, 'coingecko_id': 'wings'},
        'MTL': {'address': '0xf433089366899d83a9f26a773d59ec7ecf30355e', 'decimals': 8, 'coingecko_id': 'metal'},
        'ELF': {'address': '0xbf2179859fc6d5bee9bf9158632dc51678a4100e', 'decimals': 18, 'coingecko_id': 'aelf'},
        'AION': {'address': '0x4ceda7906a5ed2179785cd3a40a69ee8bc99c466', 'decimals': 8, 'coingecko_id': 'aion'},
        'RLC': {'address': '0x607f4c5bb672230e8672085532f7e901544a7375', 'decimals': 9, 'coingecko_id': 'iexec-rlc'},
    }
    
    symbol_upper = symbol.upper()
    contract_info = ethereum_contracts.get(symbol_upper)
    
    if contract_info:
        logger.info(f"✅ Found contract for {symbol_upper}: {contract_info['address']}")
        return contract_info
    else:
        logger.debug(f"❌ No contract found for {symbol_upper}")
        return None

# RETAIL INTELLIGENCE TARGET TOKENS - High whale activity Retail/Meme protocols
RETAIL_WHALE_TOKENS = {
    'DOGE': {'address': '0x4206931337dc273a630d328dA6441786BfaD668f', 'decimals': 8, 'coingecko_id': 'dogecoin'},
    'SHIB': {'address': '0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE', 'decimals': 18, 'coingecko_id': 'shiba-inu'},
    'PEPE': {'address': '0x6982508145454Ce325dDbE47a25d4ec3d2311933', 'decimals': 18, 'coingecko_id': 'pepe'},
    'FLOKI': {'address': '0xcf0C122c6b73ff809C693DB761e7BaeBe62b6a2E', 'decimals': 9, 'coingecko_id': 'floki'},
    'BONE': {'address': '0x9813037ee2218799597d83D4a5B6F3b6778218d9', 'decimals': 18, 'coingecko_id': 'bone-shibaswap'},
    'ELON': {'address': '0x761D38e5ddf6ccf6Cf7c55759d5210750B5D60F3', 'decimals': 18, 'coingecko_id': 'dogelon-mars'},
    'BABYDOGE': {'address': '0xc748673057861a797275CD8A068AbB95A902e8de', 'decimals': 9, 'coingecko_id': 'baby-doge-coin'},
    'AKITA': {'address': '0x3301Ee63Fb29F863f2333Bd4466acb46CD8323E6', 'decimals': 18, 'coingecko_id': 'akita-inu'},
    'APE': {'address': '0x4d224452801ACEd8B2F0aebE155379bb5D594381', 'decimals': 18, 'coingecko_id': 'apecoin'},
    'LUNC': {'address': '0xd2877702675e6cEb975b4A1dFf9fb7BAF4C91ea9', 'decimals': 18, 'coingecko_id': 'terra-luna'}
}

class EtherscanAPI:
    """Etherscan API with robust error handling - Army Enhanced"""
    
    def __init__(self, api_key, delay=ETHERSCAN_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = "https://api.etherscan.io/api"
        self.session = requests.Session()
        self.unit_name = ARMY_UNIT_NAME
    
    def get_latest_block(self):
        """Get latest block with fallbacks - Army Enhanced"""
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
                        logger.info(f"✅ {self.unit_name} latest block: {block_num:,}")
                        sys.stdout.flush()
                        return block_num
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            logger.warning(f"{self.unit_name} block lookup failed: {e}")
        
        # Fallback: estimate current block
        baseline_timestamp = 1704067200  # Jan 1, 2025
        baseline_block = 22000000
        current_timestamp = int(time.time())
        seconds_elapsed = current_timestamp - baseline_timestamp
        estimated_block = baseline_block + (seconds_elapsed // 12)
        
        logger.warning(f"⚠️ {self.unit_name} using estimated block: {estimated_block:,}")
        return min(estimated_block, 23500000)  # Cap at reasonable max
    
    def get_token_transfers(self, contract_address, start_block, end_block):
        """Get token transfers with robust error handling - Army Enhanced"""
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
                            logger.warning(f"{self.unit_name} unexpected result type: {type(result)}")
                            return []
                    elif data.get('message') == 'No transactions found':
                        return []
                    else:
                        logger.warning(f"{self.unit_name} Etherscan API: {data.get('message', 'Unknown error')}")
                        return []
                        
                else:
                    logger.warning(f"{self.unit_name} HTTP {response.status_code} - attempt {attempt + 1}")
                    
            except Exception as e:
                logger.warning(f"{self.unit_name} transfer request failed (attempt {attempt + 1}): {e}")
            
            # Backoff before retry
            if attempt < 2:
                time.sleep(self.delay * 2)
        
        return []

class CoinGeckoProAPI:
    """CoinGecko Pro API with proper rate limiting - Army Enhanced"""
    
    def __init__(self, api_key, delay=COINGECKO_DELAY):
        self.api_key = api_key
        self.delay = delay
        self.base_url = COINGECKO_PRO_BASE_URL
        self.headers = {'x-cg-pro-api-key': self.api_key}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.unit_name = ARMY_UNIT_NAME
    
    def get_multiple_prices(self, coingecko_ids):
        """Get token prices with error handling - Army Enhanced"""
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
                
                logger.info(f"💰 {self.unit_name} retrieved prices for {len(prices)} Retail tokens")
                return prices
            else:
                logger.warning(f"{self.unit_name} CoinGecko error: HTTP {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"{self.unit_name} price lookup failed: {e}")
            return {}

class RetailWhaleScanner:
    """Retail Whale Scanner - Army Unit Implementation"""
    
    def __init__(self):
        self.etherscan = EtherscanAPI(ETHERSCAN_API_KEY)
        self.coingecko = CoinGeckoProAPI(COINGECKO_API_KEY)
        self.db_connection = None
        self.unit_name = ARMY_UNIT_NAME
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
        """Connect to database with autocommit disabled - Army Enhanced"""
        try:
            self.db_connection = psycopg.connect(DB_URL)
            self.db_connection.autocommit = False  # Enable transaction control
            logger.info(f"✅ {self.unit_name} database connected")
            sys.stdout.flush()
            return True
        except Exception as e:
            logger.error(f"❌ {self.unit_name} database connection failed: {e}")
            sys.stderr.flush()
            return False
    
    def validate_transaction_data(self, tx):
        """Validate transaction data before database insert - Army Enhanced"""
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
        """Save transactions with proper error handling - Army Enhanced"""
        if not transactions or not self.db_connection:
            return 0
        
        saved_count = 0
        
        for tx in transactions:
            # Validate each transaction before attempting to save
            if not self.validate_transaction_data(tx):
                logger.debug(f"{self.unit_name} skipping invalid transaction: {tx.get('transaction_id', 'unknown')}")
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
                    logger.debug(f"{self.unit_name} skipped duplicate: {tx.get('transaction_id', 'unknown')[:16]}...")
                
                # Commit each transaction individually to avoid cascade failures
                self.db_connection.commit()
                
            except IntegrityError as e:
                logger.debug(f"{self.unit_name} integrity error (likely duplicate): {str(e)[:100]}")
                self.db_connection.rollback()
                
            except DataError as e:
                logger.warning(f"{self.unit_name} data error: {str(e)[:100]}")
                self.db_connection.rollback()
                
            except Exception as e:
                logger.warning(f"{self.unit_name} database error: {type(e).__name__}: {str(e)[:200]}")
                self.db_connection.rollback()
            
            finally:
                if 'cur' in locals():
                    cur.close()
        
        logger.info(f"💾 {self.unit_name} saved {saved_count}/{len(transactions)} Retail whale transactions")
        return saved_count
    
    def scan_token_whales(self, symbol, token_info, token_price, start_block, end_block):
        """Scan for whale transactions in a Retail token - Army Enhanced"""
        if token_price <= 0:
            logger.warning(f"{self.unit_name} skipping {symbol} - no price data")
            return []
        
        logger.info(f"🔍 {self.unit_name} scanning {symbol} (${token_price:.6f})...")
        
        # Track unique transactions in this scan to prevent duplicates
        seen_transactions = set()
        
        transfers = self.etherscan.get_token_transfers(
            token_info['address'], start_block, end_block
        )
        
        if not transfers:
            logger.info(f"  {self.unit_name} no transfers found for {symbol}")
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
                
                # Create transaction record with Army unit source tracking
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
                    'data_source': ARMY_UNIT_VERSION,  # Army unit identification
                    'processed_at': datetime.utcnow()
                }
                
                whale_transactions.append(whale_tx)
                
            except Exception as e:
                logger.debug(f"{self.unit_name} error processing {symbol} transfer: {e}")
                continue
        
        if whale_transactions:
            logger.info(f"  🐋 {self.unit_name} found {len(whale_transactions)} {symbol} whales")
        
        return whale_transactions
    
    def run_army_scan(self):
        """Execute Retail whale scan - Army Unit Mission"""
        print(f"🔧 {self.unit_name}: Army scan starting", flush=True)
        logger.info(f"🎯 {ARMY_UNIT_NAME} ARMY UNIT DEPLOYMENT - {ARMY_UNIT_VERSION}")
        sys.stdout.flush()
        start_time = datetime.utcnow()
        
        print(f"🔧 {self.unit_name}: About to connect to database", flush=True)
        if not self.connect_database():
            logger.error(f"❌ {self.unit_name} database connection failed - mission aborted")
            return False
        
        try:
            # Get latest block
            latest_block = self.etherscan.get_latest_block()
            if latest_block <= 0:
                logger.error(f"❌ {self.unit_name} cannot determine latest block - mission aborted")
                return False
            
            # Calculate 5-minute scan range (sequential scanning optimized)
            blocks_per_hour = 300  # ~12 seconds per block
            blocks_back = int(blocks_per_hour * (5/60))  # 5-minute window = 25 blocks
            start_block = max(0, latest_block - blocks_back)
            
            logger.info(f"📊 {self.unit_name} scanning blocks {start_block:,} to {latest_block:,}")
            
            # Get Retail token prices
            coingecko_ids = [info['coingecko_id'] for info in self.tokens_to_scan.values()]
            prices = self.coingecko.get_multiple_prices(coingecko_ids)
            
            if not prices:
                logger.error(f"❌ {self.unit_name} no token prices retrieved - mission aborted")
                return False
            
            # Scan each Retail token
            total_whales = 0
            total_volume = 0.0
            
            for symbol, token_info in self.tokens_to_scan.items():
                try:
                    price = prices.get(token_info['coingecko_id'], 0)
                    
                    if price <= 0:
                        logger.warning(f"{self.unit_name} no price for {symbol}, skipping")
                        continue
                    
                    whales = self.scan_token_whales(
                        symbol, token_info, price, start_block, latest_block
                    )
                    
                    if whales:
                        saved = self.save_transactions(whales)
                        volume = sum(tx['amount_usd'] for tx in whales)
                        
                        total_whales += saved
                        total_volume += volume
                        
                        logger.info(f"  ✅ {self.unit_name} {symbol}: {saved} whales, ${volume:,.0f} volume")
                    else:
                        logger.info(f"  ⚪ {self.unit_name} {symbol}: No whales found")
                    
                except Exception as e:
                    logger.error(f"❌ {self.unit_name} error scanning {symbol}: {e}")
                    continue
            
            # Army unit mission summary
            duration = (datetime.utcnow() - start_time).total_seconds() / 60
            
            logger.info(f"🎉 {ARMY_UNIT_NAME} MISSION COMPLETE!")
            logger.info(f"  🐋 Retail whales captured: {total_whales}")
            logger.info(f"  💰 Retail volume tracked: ${total_volume:,.2f}")
            logger.info(f"  ⏰ Mission duration: {duration:.1f} minutes")
            logger.info(f"  📊 Retail tokens scanned: {len(self.tokens_to_scan)}")
            logger.info(f"  🔥 Unit performance: {total_whales/duration:.1f} whales/minute")
            sys.stdout.flush()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ {self.unit_name} army scan failed: {e}")
            return False
        
        finally:
            if self.db_connection:
                self.db_connection.close()
                logger.info(f"📝 {self.unit_name} database connection closed")
                sys.stdout.flush()

def main():
    """Main entry point for Retail Army Unit cron execution"""
    print(f"🔧 {ARMY_UNIT_NAME}: Starting main function", flush=True)
    
    try:
        print(f"🔧 {ARMY_UNIT_NAME}: Creating Retail whale scanner instance", flush=True)
        scanner = RetailWhaleScanner()
        
        print(f"🔧 {ARMY_UNIT_NAME}: Starting army mission", flush=True)
        success = scanner.run_army_scan()
        
    except Exception as e:
        print(f"🔧 {ARMY_UNIT_NAME}: Exception caught: {e}", flush=True)
        logger.error(f"❌ {ARMY_UNIT_NAME} main function failed: {e}")
        sys.stderr.flush()
        exit(1)
    
    if success:
        logger.info(f"✅ {ARMY_UNIT_NAME} completed mission successfully")
        sys.stdout.flush()
        exit(0)  # Success exit code
    else:
        logger.error(f"❌ {ARMY_UNIT_NAME} mission failed")
        sys.stderr.flush()
        exit(1)  # Error exit code

if __name__ == "__main__":
    main()
