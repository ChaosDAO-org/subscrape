__author__ = 'spazcoin@gmail.com @spazvt'

import json
import logging
from pathlib import Path
import pytest
import sys
import time

repo_root = Path(__file__).parent.parent.resolve()
sys.path.append(str(repo_root))
import subscrape


@pytest.mark.asyncio
async def test__extract_addresses_calling_contract_method():
    # Specifically, adding liquidity to the Solarbeam DEX
    config = {
        "moonriver": {
            "transactions": {
                "0xaa30ef758139ae4a7f798112902bf6d65612045f": [
                    "0xe8e33700"
                ],
                "_filter": [{"blockNumber": [{">=": 992929}, {"<=": 993002}]}]
            }
        }
    }

    logging.info(f"begin 'test__process_method_in_transaction' scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 2
    assert ('0x48630c63beba19bbdc6e57d7d9c98735f5dd3d37' in items_scraped)
    assert ('0xad2b8e18cc7bddde1fe7e254d78abf1188b6c8f4' in items_scraped)


@pytest.mark.asyncio
async def test__decode_token_swap_transaction():
    test_acct = "0xBa4123F4b2da090aeCef69Fd0946D42Ecd4C788E"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"timeStamp": [{">=": 1638169446}, {"<=": 1638248544}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction' scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert items_scraped[0] >= 2
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x066f0e5a15d4c0094caa83addf6e60ea35c21b9212dfdc998ca89809307c3b82':
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ZLK'
            assert tx['output_a_token_symbol'] == 'USDC'
            assert_value_within_range(tx['input_a_quantity'], 7.743865640456116)
            assert_value_within_range(tx['output_a_quantity'], 22.428698)
        elif tx['hash'] == '0x921e89b531d8ad251e065a5cedc2fdaeacd3ca5fd9120bfbef5c2c9054b22263':
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'USDC'
            assert_value_within_range(tx['input_a_quantity'], 1.519)
            assert_value_within_range(tx['output_a_quantity'], 465.032663)
        else:
            continue


def get_archived_transactions_from_json(address, chain='moonriver'):
    """Return a list of all transactions (dict by timestamp) read from the JSON file generated when scraping.

    :param address: the moonriver/moonbeam account number of interest. This could be a basic account, or a contract
    address, depending on the kind of transactions being analyzed.
    :type address: str
    :param chain: name of the specific chain that was scraped
    :type chain: str
    """
    # Import the transactions from JSON file
    json_file_name = f'{chain}_{address}.json'
    json_file_path = Path(repo_root / 'data' / 'parachains' / json_file_name).resolve()
    assert json_file_path.is_file()
    with json_file_path.open('r', encoding="UTF-8") as input_file:
        data = json.load(input_file)
    return data


def assert_value_within_range(actual, expected, tolerance_percent=1):
    """Return a list of all transactions (dict by timestamp) read from the JSON file generated when scraping.

    :param actual: actual float value received
    :type actual: float
    :param expected: expected float value to test against
    :type expected: float
    :param tolerance_percent: percent tolerance around the expected value
    :type tolerance_percent: float
    """
    if isinstance(actual, float):
        actual_float = actual
    elif isinstance(actual, str):
        actual_float = float(actual)
    else:
        assert False
    if isinstance(expected, float):
        expected_float = expected
    elif isinstance(expected, str):
        expected_float = float(expected)
    else:
        assert False
    tolerance = expected_float * tolerance_percent / 100
    lower_limit = expected_float - tolerance
    upper_limit = expected_float + tolerance
    assert actual_float >= lower_limit
    assert actual_float <= upper_limit
