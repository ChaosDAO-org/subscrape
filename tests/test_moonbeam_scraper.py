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

test_scope = "all"      # 'all', 'swaps', 'liquidity', 'kbtc'
new_only = False


@pytest.mark.skipif(new_only or test_scope not in {'all'}, reason="reduce API queries during debug/dev")
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


@pytest.mark.skipif(new_only or test_scope not in {'all'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__no_entries_returned():
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

    logging.info(f"begin 'test__no_entries_returned'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) == 0
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver', file_expected=False)
    assert transactions is None


# #############################################################
# ######### TOKEN SWAP TESTS ##################################
# #############################################################

@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Solarbeam__swapExactTokensForTokens():
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

    logging.info(f"begin 'test__decode_token_swap_tx_Solarbeam__swapExactTokensForTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 2
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x066f0e5a15d4c0094caa83addf6e60ea35c21b9212dfdc998ca89809307c3b82':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ZLK'
            assert tx['output_a_token_symbol'] == 'USDC'
            _assert_value_within_range(tx['input_a_quantity'], 7.743865640456116)
            _assert_value_within_range(tx['output_a_quantity'], 22.428698)
        elif tx['hash'] == '0x921e89b531d8ad251e065a5cedc2fdaeacd3ca5fd9120bfbef5c2c9054b22263':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'USDC'
            _assert_value_within_range(tx['input_a_quantity'], 1.519)
            _assert_value_within_range(tx['output_a_quantity'], 465.032663)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Zenlink__swapExactTokensForTokens():
    # also testing: swap transaction on Zenlink DEX. two hops.
    test_acct = "0x2b46c40b6d1f4d77a6719f92864cf40bb049e366"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 2228825}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_Zenlink__swapExactTokensForTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xc03c6597620bc94189840ec0ac7927293326394e0ed8261f7d82cb2ebe1dd17a':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'xcKSM'
            assert tx['output_a_token_symbol'] == 'USDC'
            _assert_value_within_range(tx['input_a_quantity'], 5)
            _assert_value_within_range(tx['output_a_quantity'], 310.474984)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Zenlink__swapTokensForExactTokens():
    # also testing: swap transaction on Zenlink DEX.
    test_acct = "0x79a8a9ff5717248a5fc0f00cc9920bd4bba77823"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 1224237}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_Zenlink__swapTokensForExactTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x3b10057c7d19702732e48d7ec63f1b4f2299dccef79780263d28edbe61e2ed29':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'USDC'
            assert tx['output_a_token_symbol'] == 'WBTC'
            _assert_value_within_range(tx['input_a_quantity'], 462.79385)
            _assert_value_within_range(tx['output_a_quantity'], 0.01)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Huckleberry__swapTokensForExactTokens():
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

    logging.info(f"begin 'test__decode_token_swap_tx_Huckleberry__swapTokensForExactTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xbdacc65d82e8e7273e42ba3c0c33d3ec884809087c718fcd4076709577cbd2f7':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'BNB.m'
            assert tx['output_a_token_symbol'] == 'FTM.m'
            _assert_value_within_range(tx['input_a_quantity'], 0.038442263813382655)
            _assert_value_within_range(tx['output_a_quantity'], 51.8294)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Huckleberry__swapExactTokensForTokensSupportingFeeOnTransferTokens():
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

    logging.info(f"begin 'test__decode_token_swap_tx_Huckleberry__swapExactTokensForTokensSupportingFeeOnTransferTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    logging.info('Note: for this specific unit test, subscrape will emit a warning like "expected log decoded output'
                 ' quantity 3.02e-06 to be within 20% of the requested tx output quantity 0.0 but its not."'
                 ' Evidently the original contract call set amountOutMin=0 so this is expected behavior.')
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xbc065d9aa4fd90a0fb1df3ddfaab633cd3866e5dc069c6ce47f33593d3aa8972':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'BTC.m'
            _assert_value_within_range(tx['input_a_quantity'], 0.008232571613605909)
            _assert_value_within_range(tx['output_a_quantity'], 0.00000302)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Solarbeam__swapExactTokensForETH():
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

    logging.info(f"begin 'test__decode_token_swap_tx_Solarbeam__swapExactTokensForETH'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x0f379919b8dff50c1d83cf92c3aa7eca75e5558251ecf99e9e7fb660faf74c95':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'FRAX'
            assert tx['output_a_token_symbol'] == 'WMOVR'
            _assert_value_within_range(tx['input_a_quantity'], 100)
            _assert_value_within_range(tx['output_a_quantity'], 11.526796881289545)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Huckleberry__swapExactETHForTokens():
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

    logging.info(f"begin 'test__decode_token_swap_tx_Huckleberry__swapExactETHForTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xf4a51bb43a24e5bf7047d563ec831762659a1535bdbcb056764ac4298f4d3b08':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'DOT.m'
            _assert_value_within_range(tx['input_a_quantity'], 1)
            _assert_value_within_range(tx['output_a_quantity'], 1.2598567127)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Huckleberry__swapTokensForExactETH():
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

    logging.info(f"begin 'test__decode_token_swap_tx_Huckleberry__swapTokensForExactETH'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x1b036cd6d161622a5f610680b2fc15aee96103a4a884d9338ecb59f65b31753f':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'FTM.m'
            assert tx['output_a_token_symbol'] == 'WMOVR'
            _assert_value_within_range(tx['input_a_quantity'], 1.507652227912820355)
            _assert_value_within_range(tx['output_a_quantity'], 0.06)
        else:
            continue
    assert transaction_found


# @pytest.mark.skipif(not new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
# @pytest.mark.asyncio
# async def test__decode_token_swap_tx__swapETHForExactTokens():
#     # also testing:
#     test_acct = "XXXXXXXXXXXXX"
#     config = {
#         "moonriver": {
#             "account_transactions": {
#                 "accounts": [
#                     test_acct
#                 ],
#                 "_filter": [{"blockNumber": [{"==": XXXXXXX}]}]
#             }
#         }
#     }
#
#     logging.info(f"begin 'test__decode_token_swap_tx__swapETHForExactTokens'"
#                  f" scraping at {time.strftime('%X')}")
#     items_scraped = await subscrape.scrape(config)
#     assert len(items_scraped) >= 1
#     transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
#     transaction_found = False
#     for timestamp in transactions:
#         tx = transactions[timestamp]
#         if tx['hash'] == 'XXXXXXXXXXXXXXXXX':
#             transaction_found = True
#             logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
#             assert tx['input_a_token_symbol'] == 'XXXX'
#             assert tx['output_a_token_symbol'] == 'XXXX'
#             _assert_value_within_range(tx['input_a_quantity'], 1111111)
#             _assert_value_within_range(tx['output_a_quantity'], 1111111)
#         else:
#             continue
#     assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Zenlink__swapExactNativeCurrencyForTokens():
    # also testing: swap transaction on Zenlink DEX.
    test_acct = "0xe1fa699860444be91d366c21de8fef56e3dec77a"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 2971916}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_Zenlink__swapExactNativeCurrencyForTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x20f12bed5fa0c6c61a037196ec344b24e6f473dc54dd932492c7a7643eb33251':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'xcKSM'
            _assert_value_within_range(tx['input_a_quantity'], 11.1)
            _assert_value_within_range(tx['output_a_quantity'], 3.727989001715)
        else:
            continue
    assert transaction_found



@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Zenlink__swapExactTokensForNativeCurrency():
    # also testing: swap transaction on Zenlink DEX.
    test_acct = "0x335391f2006c318dc318230bdec020031d7dac75"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 1221284}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_Zenlink__swapExactTokensForNativeCurrency'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xd1139087f55ac1b9d377c408afed9ca86478b725f7266d7c5d15e22ba5cd1a81':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'USDC'
            assert tx['output_a_token_symbol'] == 'WMOVR'
            _assert_value_within_range(tx['input_a_quantity'], 1185.327082)
            _assert_value_within_range(tx['output_a_quantity'], 6.056467880187909967)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Zenlink__swapTokensForExactNativeCurrency():
    # also testing: swap transaction on Zenlink DEX.
    test_acct = "0x8fbebbb93019dc8e56630507e206d8ad00842d41"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 1219176}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_Zenlink__swapTokensForExactNativeCurrency'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xf72916c26906789b1b6768d8347652e47d042181f8a1545ebbe80018fa2ce111':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ZLK'
            assert tx['output_a_token_symbol'] == 'WMOVR'
            _assert_value_within_range(tx['input_a_quantity'], 963.663306508578689591)
            _assert_value_within_range(tx['output_a_quantity'], 10)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_Zenlink__swapNativeCurrencyForExactTokens():
    # also testing: swap transaction on Zenlink DEX.
    test_acct = "0x9191c75bb0681a71f7d254484f2eb749c8934dac"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 1218897}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_Zenlink__swapNativeCurrencyForExactTokens'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0xc28ed3d98f9e6404828b73280eda5db1cc19aaca70dac03fed62bfdbf2273431':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'ZLK'
            _assert_value_within_range(tx['input_a_quantity'], 10.591632213710223118)
            _assert_value_within_range(tx['output_a_quantity'], 1000)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(not new_only or test_scope not in {'kbtc'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_token_swap_tx_SolarbeamStableSwap__swap():
    # also testing: StableSwap AMM on Solarbeam DEX.
    test_acct = "0x27e6a60146c5341d2e5577b219a2961f2d180579"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3476511}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_token_swap_tx_SolarbeamStableSwap__swap'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x66d7f4a07c551724a4434be9fee0271638686db4f7d9232d74c6bfc94f8ee762':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'xcKBTC'
            assert tx['output_a_token_symbol'] == 'WBTC'
            _assert_value_within_range(tx['input_a_quantity'], 0.00275235)
            _assert_value_within_range(tx['output_a_quantity'], 0.00277169)
        else:
            continue
    assert transaction_found


# @pytest.mark.skipif(not new_only or test_scope not in {'all', 'swaps'}, reason="reduce API queries during debug/dev")
# @pytest.mark.asyncio
# async def test__decode_token_swap_tx__():
#     # also testing: swap transaction on Zenlink DEX.
#     test_acct = "XXXXXXXXXXXXX"
#     config = {
#         "moonriver": {
#             "account_transactions": {
#                 "accounts": [
#                     test_acct
#                 ],
#                 "_filter": [{"blockNumber": [{"==": XXXXXXX}]}]
#             }
#         }
#     }
#
#     logging.info(f"begin 'test__decode_token_swap_tx__'"
#                  f" scraping at {time.strftime('%X')}")
#     items_scraped = await subscrape.scrape(config)
#     assert len(items_scraped) >= 1
#     transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
#     transaction_found = False
#     for timestamp in transactions:
#         tx = transactions[timestamp]
#         if tx['hash'] == 'XXXXXXXXXXXXXXXXX':
#             transaction_found = True
#             logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
#             assert tx['input_a_token_symbol'] == 'XXXX'
#             assert tx['output_a_token_symbol'] == 'XXXX'
#             _assert_value_within_range(tx['input_a_quantity'], 1111111)
#             _assert_value_within_range(tx['output_a_quantity'], 1111111)
#         else:
#             continue
#     assert transaction_found


# #############################################################
# ######### ADD LIQUIDITY TESTS ###############################
# #############################################################

@pytest.mark.skipif(new_only or test_scope not in {'all', 'liquidity'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_add_liquidity_tx_Zenlink__addLiquidity():
    # also testing: Zenlink transactions
    test_acct = "0xa3d2c4af7496069d264e9357f9f39f79a656a1c8"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 2411203}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_add_liquidity_tx_Zenlink__addLiquidity'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x25ddcd67e7b2609e06f0cb2e5fd3aa8791997cce751ec0df796a363b32ae44f3':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ZLK'
            assert tx['input_b_token_symbol'] == 'USDC'
            assert tx['output_a_token_symbol'] == 'ZLK-LP'
            _assert_value_within_range(tx['input_a_quantity'], 1065.32333333333333333)
            _assert_value_within_range(tx['input_b_quantity'], 68.583532)
            _assert_value_within_range(tx['output_a_quantity'], 0.000261425699767385)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'liquidity'}, reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_add_liquidity_tx_Solarbeam__addLiquidityETH():
    # also testing:
    test_acct = "0xc794047d59f11bef4035241ab403c9a419d7da8d"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3513919}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_add_liquidity_tx_Solarbeam__addLiquidityETH'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x6c56b95468fc04805d4d714ddd8edd46e61548a095460b7f912de7303383c3bf':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'MFAM'
            assert tx['input_b_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'SLP'
            _assert_value_within_range(tx['input_a_quantity'], 109972.053560515580176075)
            _assert_value_within_range(tx['input_b_quantity'], 23.840224856355448048)
            _assert_value_within_range(tx['output_a_quantity'], 1520.682022451843464574)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'liquidity'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_add_liquidity_tx_Zenlink__addLiquidityNativeCurrency():
    # also testing: Zenlink transactions
    test_acct = "0x725f5b2e92164c38ef25a70f0807b71a7f0e770a"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 2320610}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_add_liquidity_tx_Zenlink__addLiquidityNativeCurrency'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x22ad8fa7066c9b9b98a0c4bb2dd00a169cd5f5380aa07b3c322450e2701f2d15':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'xcKSM'
            assert tx['input_b_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'ZLK-LP'
            _assert_value_within_range(tx['input_a_quantity'], 3.635995421334)
            _assert_value_within_range(tx['input_b_quantity'], 14.999999999996282704)
            _assert_value_within_range(tx['output_a_quantity'], 0.007208972847761851)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'liquidity'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_add_liquidity_tx_Zenlink__addLiquiditySingleNativeCurrency():
    test_acct = "0x9f7aa4f003817352e9770e579be7efcd37ba1990"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 1222466}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_add_liquidity_tx_Zenlink__addLiquiditySingleNativeCurrency'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x67f837a13804964808ff8d92a20ecadd9087ec352a2ce0524667c52022775169':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WMOVR'
            assert tx['output_a_token_symbol'] == 'ZLK-LP'
            _assert_value_within_range(tx['input_a_quantity'], 7.523520565458797)
            _assert_value_within_range(tx['output_a_quantity'], 0.000051110515327605)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'liquidity'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_add_liquidity_tx_Zenlink__addLiquiditySingleToken():
    # Testing Zenlink exchange ETH for vETH and receive ZLK-LP
    # Oddly this transaction only specifies ETH->vETH in the token path instead of full ETH->vETH->ZLK-LP.
    # The series of events are ETH->vETH, send vETH back to acct, then provide both ETH and vETH for ZLK-LP.
    test_acct = "0x0a83985e4a6e8dae2b67bed4f2d9268f6806ce00"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 2411562}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_add_liquidity_tx_Zenlink__addLiquiditySingleToken'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x311d38bf46501961abdee8c9d808f9990fb7d91bd658039022bedd05ccad4581':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ETH'
            assert tx['output_a_token_symbol'] == 'ZLK-LP'
            _assert_value_within_range(tx['input_a_quantity'], 0.04364793008913)
            _assert_value_within_range(tx['output_a_quantity'], 0.023807051928692573)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(not new_only or test_scope not in {'kbtc'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_add_liquidity_tx_SolarbeamStableSwap__kbtc_stableswap_solarbeam():
    # also testing: Add liquidity to kBTC-BTC StableSwap on Solarbeam DEX.
    test_acct = "0xc365926c71dae2c7e39d176c9406239318301a3c"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3457655}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_add_liquidity_tx_SolarbeamStableSwap__kbtc_stableswap_solarbeam'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x75cb660519977cb8a98334b717d883543ccc44c1f6473e72b8d844e00e84a61f':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'WBTC'
            assert tx['output_a_token_symbol'] == 'kBTC-BTC'
            _assert_value_within_range(tx['input_a_quantity'], 1.71863)
            _assert_value_within_range(tx['output_a_quantity'], 1.712116389853722404)
        else:
            continue
    assert transaction_found


# @pytest.mark.skipif(not new_only or test_scope not in {'all', 'liquidity'}, reason="reduce API queries during debug/dev")
# @pytest.mark.asyncio
# async def test__decode_add_liquidity_tx__():
#     # also testing:
#     test_acct = "XXXXXXXXXX"
#     config = {
#         "moonriver": {
#             "account_transactions": {
#                 "accounts": [
#                     test_acct
#                 ],
#                 "_filter": [{"blockNumber": [{"==": xxxxxxxxxx}]}]
#             }
#         }
#     }
#
#     logging.info(f"begin 'test__decode_add_liquidity_tx__XXXXXXXXXXXXXXXX'"
#                  f" scraping at {time.strftime('%X')}")
#     items_scraped = await subscrape.scrape(config)
#     assert len(items_scraped) >= 1
#     transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
#     transaction_found = False
#     for timestamp in transactions:
#         tx = transactions[timestamp]
#         if tx['hash'] == 'XXXXXXXXXXX':
#             transaction_found = True
#             logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
#             assert tx['input_a_token_symbol'] == 'xxxxxx'
#             assert tx['input_b_token_symbol'] == 'xxxxxx'
#             assert tx['output_a_token_symbol'] == 'xxxxxxx'
#             _assert_value_within_range(tx['input_a_quantity'], xxxxxx)
#             _assert_value_within_range(tx['input_b_quantity'], xxxxxxx)
#             _assert_value_within_range(tx['output_a_quantity'], xxxxxxxxx)
#         else:
#             continue
#     assert transaction_found


# #############################################################
# ######### REMOVE LIQUIDITY TESTS ############################
# #############################################################

# @pytest.mark.skipif(not new_only or test_scope not in {'all', 'liquidity'}, reason="reduce API queries during debug/dev")
# @pytest.mark.asyncio
# async def test__decode_remove_liquidity_tx__():
#     # also testing: Zenlink transactions
#     test_acct = "XXXXXXXXXX"
#     config = {
#         "moonriver": {
#             "account_transactions": {
#                 "accounts": [
#                     test_acct
#                 ],
#                 "_filter": [{"blockNumber": [{"==": }]}]
#             }
#         }
#     }
#
#     logging.info(f"begin 'test__decode_remove_liquidity_tx__XXXXXXXXXXXXXXXX'"
#                  f" scraping at {time.strftime('%X')}")
#     items_scraped = await subscrape.scrape(config)
#     assert len(items_scraped) >= 1
#     transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
#     transaction_found = False
#     for timestamp in transactions:
#         tx = transactions[timestamp]
#         if tx['hash'] == 'XXXXXXXXXXX':
#             transaction_found = True
#             logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
#             assert tx['input_a_token_symbol'] == 'ZLK-LP'
#             assert tx['output_a_token_symbol'] == ''
#             assert tx['output_b_token_symbol'] == ''
#             _assert_value_within_range(tx['input_a_quantity'], )
#             _assert_value_within_range(tx['output_a_quantity'], )
#             _assert_value_within_range(tx['output_b_quantity'], )
#         else:
#             continue
#     assert transaction_found


@pytest.mark.skipif(new_only or test_scope not in {'all', 'liquidity'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_remove_liquidity_tx_Zenlink__removeLiquidityNativeCurrency():
    # also testing: Zenlink transactions
    test_acct = "0x96cc80292fa3a7045611eb84ae09df8bd15936d2"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3430477}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_remove_liquidity_tx_Zenlink__removeLiquidityNativeCurrency'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x7858cd252b4e6a56e22491baa5e6a2a8db2ea53f14b33e469d342daaef6bdab5':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'ZLK-LP'
            assert tx['output_a_token_symbol'] == 'xcRMRK'
            assert tx['output_b_token_symbol'] == 'WMOVR'
            _assert_value_within_range(tx['input_a_quantity'], 0.001697922096285993)
            _assert_value_within_range(tx['output_a_quantity'], 31.4698637357)
            _assert_value_within_range(tx['output_b_quantity'], 9.825816067932214113)
        else:
            continue
    assert transaction_found


@pytest.mark.skipif(not new_only or test_scope not in {'kbtc'},
                    reason="reduce API queries during debug/dev")
@pytest.mark.asyncio
async def test__decode_remove_liquidity_tx_SolarbeamStableSwap__kbtc_stableswap_solarbeam():
    # also testing: Remove liquidity from kBTC-BTC StableSwap on Solarbeam DEX.
    test_acct = "0xb4c9531a60e252c871d51923bc9f153f1d371ca8"
    config = {
        "moonriver": {
            "account_transactions": {
                "accounts": [
                    test_acct
                ],
                "_filter": [{"blockNumber": [{"==": 3460284}]}]
            }
        }
    }

    logging.info(f"begin 'test__decode_remove_liquidity_tx_SolarbeamStableSwap__kbtc_stableswap_solarbeam`'"
                 f" scraping at {time.strftime('%X')}")
    items_scraped = await subscrape.scrape(config)
    assert len(items_scraped) >= 1
    transactions = _get_archived_transactions_from_json(test_acct, 'moonriver')
    transaction_found = False
    for timestamp in transactions:
        tx = transactions[timestamp]
        if tx['hash'] == '0x17bef7512deaec0662ee5a90193175b8965247da826b7ec8814396175efdefe5':
            transaction_found = True
            logging.debug(f'for hash {tx["hash"]} the full transaction is {tx}')
            assert tx['input_a_token_symbol'] == 'kBTC-BTC'
            assert tx['output_a_token_symbol'] == 'WBTC'
            _assert_value_within_range(tx['input_a_quantity'], 0.119747)
            _assert_value_within_range(tx['output_a_quantity'], 0.12020283)
        else:
            continue
    assert transaction_found


# #############################################################
# ######### HELPER METHODS ####################################
# #############################################################


def _get_archived_transactions_from_json(address, chain='moonriver', file_expected=True):
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


def _assert_value_within_range(actual, expected, tolerance_percent=1):
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
