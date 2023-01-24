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
    # Specifically, adding liquidity to the Solarbeam DEX. Also test filter range on "blockNumber"
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
    assert ('0x48630c63beba19bbdc6e57d7d9c98735f5dd3d37' in items_scraped)  # aROME-FRAX LP
    assert ('0xad2b8e18cc7bddde1fe7e254d78abf1188b6c8f4' in items_scraped)


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapExactTokensForTokens():
    # also testing: swap transaction on Solarbeam DEX. filter range on "timeStamp"
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

    logging.info(f"begin 'test__decode_token_swap_transaction__swapExactTokensForTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 2
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x066f0e5a15d4c0094caa83addf6e60ea35c21b9212dfdc998ca89809307c3b82':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ZLK'
            assert tx['output_a_token_symbol'] == 'USDC'
            assert_value_within_range(tx['input_a_quantity'], 7.743865640456116)
            assert_value_within_range(tx['output_a_quantity'], 22.428698)
        elif tx['hash'] == '0x921e89b531d8ad251e065a5cedc2fdaeacd3ca5fd9120bfbef5c2c9054b22263':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'USDC'
            assert_value_within_range(tx['input_a_quantity'], 1.519)
            assert_value_within_range(tx['output_a_quantity'], 465.032663)
        else:
            continue
    assert transaction_found


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapExactTokensForETH():
    # also testing: swap transaction on Solarbeam DEX. filter '==' on blockNumber
    test_acct = "0x299cd1c791464827ddfb147612244a2c59da91a0"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3471171}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction__swapExactTokensForETH'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x0f379919b8dff50c1d83cf92c3aa7eca75e5558251ecf99e9e7fb660faf74c95':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'FRAX'
            assert tx['output_a_token_symbol'] == 'WMOVR'
            assert_value_within_range(tx['input_a_quantity'], 100)
            assert_value_within_range(tx['output_a_quantity'], 11.526796881289545)
        else:
            continue
    assert transaction_found


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapExactETHForTokens():
    # also testing: swap transaction on Huckleberry DEX. "blockNumber" range in <= >= order. two hop tx
    test_acct = "0x8e7fbb49f436d0e8a50c02f631e729a57a9a0aca"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"<=": 3421768}, {">=": 3421760}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction__swapExactETHForTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xf4a51bb43a24e5bf7047d563ec831762659a1535bdbcb056764ac4298f4d3b08':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'DOT.m'
            assert_value_within_range(tx['input_a_quantity'], 1)
            assert_value_within_range(tx['output_a_quantity'], 1.2598567127)
        else:
            continue
    assert transaction_found


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapExactTokensForTokensSupportingFeeOnTransferTokens():
    # also testing: swap transaction on Huckleberry DEX. filter '==' on blockNumber
    test_acct = "0x85cf0915c8d3695b03da739e3eaefd5388eb5eef"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3313868}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction__swapExactTokensForTokensSupportingFeeOnTransferTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xbc065d9aa4fd90a0fb1df3ddfaab633cd3866e5dc069c6ce47f33593d3aa8972':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'BTC.m'
            assert_value_within_range(tx['input_a_quantity'], 0.008232571613605909)
            assert_value_within_range(tx['output_a_quantity'], 0.00000302)
        else:
            continue
    assert transaction_found


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapTokensForExactETH():
    # also testing: swap transaction on Huckleberry DEX. "blockNumber" range in <= >= with equal block number. Two hops.
    test_acct = "0xfc2f3c2b6872d6ad347e78f096026274326ab081"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"<=": 3427502}, {">=": 3427502}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction__swapTokensForExactETH'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x1b036cd6d161622a5f610680b2fc15aee96103a4a884d9338ecb59f65b31753f':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'FTM.m'
            assert tx['output_a_token_symbol'] == 'WMOVR'
            assert_value_within_range(tx['input_a_quantity'], 1.507652227912820355)
            assert_value_within_range(tx['output_a_quantity'], 0.06)
        else:
            continue
    assert transaction_found


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapTokensForExactTokens():
    # also testing: swap transaction on Huckleberry DEX. "timeStamp" range in <= >= order. Two hop.
    test_acct = "0xfc2f3c2b6872d6ad347e78f096026274326ab081"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"timeStamp": [{"<=": 1672581700}, {">=": 1672581680}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction__swapTokensForExactTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xbdacc65d82e8e7273e42ba3c0c33d3ec884809087c718fcd4076709577cbd2f7':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'BNB.m'
            assert tx['output_a_token_symbol'] == 'FTM.m'
            assert_value_within_range(tx['input_a_quantity'], 0.038442263813382655)
            assert_value_within_range(tx['output_a_quantity'], 51.8294)
        else:
            continue
    assert transaction_found


@pytest.mark.asyncio
async def test__decode_token_swap_transaction__swapTokensForExactTokens__no_entries_returned():
    test_acct = "0xa00654efb77c7861f42b32de3590e9a51a5aff64"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"timeStamp": [{"<=": 1672603300}, {">=": 1672603280}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_transaction__swapTokensForExactTokens__no_entries_returned'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) == 0
    transactions = get_archived_transactions_from_json(test_acct, 'moonriver', file_expected=False)
    assert transactions is None


def get_archived_transactions_from_json(address, chain='moonriver', file_expected=True):
    """Return a list of all transactions (dict by timestamp) read from the JSON file generated when scraping.

    :param address: the moonriver/moonbeam account number of interest. This could be a basic account, or a contract
    address, depending on the kind of transactions being analyzed.
    :type address: str
    :param chain: name of the specific chain that was scraped
    :type chain: str
    :param file_expected: is a JSON file expected to be found?
    :type file_expected: bool
    :returns: json structure with the scraped transactions
    :rtype: dict
    """
    # Import the transactions from JSON file
    json_file_name = f'{chain}_{address}.json'
    json_file_path = Path(repo_root / 'data' / 'parachains' / json_file_name).resolve()
    data = None
    if file_expected:
        assert json_file_path.is_file()
        with json_file_path.open('r', encoding="UTF-8") as input_file:
            data = json.load(input_file)
    return data


def assert_value_within_range(actual, expected, tolerance_percent=1):
    """Verify that an actual float value is within 'tolerance' % of expected float value (to avoid exact comparisons)

    :param actual: actual float value received
    :type actual: float
    :param expected: expected float value to test against
    :type expected: float
    :param tolerance_percent: percent tolerance around the expected value
    :type tolerance_percent: float
    """
    if isinstance(actual, float):
        actual_float = actual
    elif isinstance(actual, str) or isinstance(actual, int):
        actual_float = float(actual)
    else:
        assert False
    if isinstance(expected, float):
        expected_float = expected
    elif isinstance(expected, str) or isinstance(expected, int):
        expected_float = float(expected)
    else:
        assert False
    tolerance = expected_float * tolerance_percent / 100
    lower_limit = expected_float - tolerance
    upper_limit = expected_float + tolerance
    assert actual_float >= lower_limit
    assert actual_float <= upper_limit
