# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal

from ...config import config
from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import UnexpectedTypeError

WALLET = "Ledger Live"

def parse_ledger_live(data_rows, parser, _filename):
    for data_row in data_rows:
        if  config.args.debug:
            sys.stderr.write("%sconv: row[%s] %s\n" % (
                Fore.YELLOW, parser.in_header_row_num + data_row.line_num, data_row))

        if data_row.parsed:
            continue

        try:
            parse_ledger_live_row(data_rows, parser, data_row)
        except DataParserError as e:
            data_row.failure = e


def parse_ledger_live_row(data_rows, parser, data_row):
    
    in_row = data_row.in_row
    data_row.timestamp = DataParser.parse_timestamp(in_row[0])
    tx_hash = in_row[5]

    if in_row[2] == "IN":

        # Uniswap transactions will have IN/OUT but need to be considered as Trades
        other_tx_found, quantity, fee_quantity, asset = find_same_tx(data_rows, tx_hash, "OUT")
        if other_tx_found and quantity:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_TRADE,
                                                     data_row.timestamp,
                                                     buy_quantity=in_row[3],
                                                     buy_asset=in_row[1],
                                                     sell_quantity=quantity,
                                                     sell_asset=asset,
                                                     fee_quantity=fee_quantity,
                                                     fee_asset=asset,
                                                     wallet=WALLET)

        elif in_row[4]:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                     data_row.timestamp,
                                                     buy_quantity=Decimal(in_row[3]) + \
                                                                  Decimal(in_row[4]),
                                                     buy_asset=in_row[1],
                                                     fee_quantity=in_row[4],
                                                     fee_asset=in_row[1],
                                                     wallet=WALLET)
        else:
            # ERC-20 tokens don't include fees
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                     data_row.timestamp,
                                                     buy_quantity=in_row[3],
                                                     buy_asset=in_row[1],
                                                     wallet=WALLET)
                                                    
    elif in_row[2] == "OUT":

        # Uniswap transactions will have IN/OUT but need to be considered as Trades
        other_tx_found, quantity, fee_quantity, asset = find_same_tx(data_rows, tx_hash, "IN")
        if other_tx_found and quantity:
            pass
        elif in_row[4]:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                     data_row.timestamp,
                                                     sell_quantity=Decimal(in_row[3]) - \
                                                                   Decimal(in_row[4]),
                                                     sell_asset=in_row[1],
                                                     fee_quantity=in_row[4],
                                                     fee_asset=in_row[1],
                                                     wallet=WALLET)
        else:
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                     data_row.timestamp,
                                                     sell_quantity=in_row[3],
                                                     sell_asset=in_row[1],
                                                     wallet=WALLET)
    else:
        raise UnexpectedTypeError(2, parser.in_header[2], in_row[2])


def find_same_tx(data_rows, tx_hash, tx_type):
    other_tx_found = False
    quantity = None
    fee_quantity = None
    asset = ""

    data_rows = [data_row for data_row in data_rows
                 if data_row.in_row[5] == tx_hash 
                 and not data_row.parsed] 
    for data_row in data_rows:
        if tx_type == data_row.in_row[2]:
            other_tx_found = True
            quantity = data_row.in_row[3]
            fee_quantity = data_row.in_row[4]
            asset = data_row.in_row[1]
            # data_row.parsed = True
            break

    return other_tx_found, quantity, fee_quantity, asset


DataParser(DataParser.TYPE_WALLET,
           "Ledger Live",
           ['Operation Date', 'Currency Ticker', 'Operation Type', 'Operation Amount',
            'Operation Fees', 'Operation Hash', 'Account Name', 'Account xpub'],
           worksheet_name="Ledger",
           all_handler=parse_ledger_live)

DataParser(DataParser.TYPE_WALLET,
           "Ledger Live",
           ['Operation Date', 'Currency Ticker', 'Operation Type', 'Operation Amount',
            'Operation Fees', 'Operation Hash', 'Account Name', 'Account id'],
           worksheet_name="Ledger",
           all_handler=parse_ledger_live)
