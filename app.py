import asyncio
import os
import argparse
import zmq
import zmq.asyncio
import json
import logging
import time
import traceback
import inspect
import re
import sys
import csv
import tws
import struct
from zmapi.codes import error
from zmapi.connector import ControllerBase
from zmapi.exceptions import *
from zmapi.utils import lru_cache
import aiohttp
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime
from datetime import datetime
from tws_codes import *
from constants import *
from collections import defaultdict

################################ GLOBAL STATE #################################

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"

# placeholder for Logger
L = None

################################### HELPERS ###################################

def get_next_ib_tid():
    ib_tid = g.next_valid_id
    g.next_valid_id += 1
    return ib_tid

###############################################################################

class MDController(ControllerBase):

    def __init__(self, ctx, addr):
        super().__init__("MD", ctx, addr)

    @ControllerBase.handler()
    async def get_status(self, ident, msg):
        status = {
            "name": "ibtws-md",
            "connector_name": "ibtws",
            "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
        }
        return [status]

    # @ControllerBase.handler()
    # async def get_ticker_fields(self, ident, msg):
    #     return TICKER_FIELDS

    @ControllerBase.handler()
    async def get_ticker_info(self, ident, msg):
        ticker = msg["content"]["ticker"]
        res = await g.tws_client.req_contract_details(ticker)
        return res

    @ControllerBase.handler()
    async def list_capabilities(self, ident, msg):
        return MD_CAPABILITIES

    @ControllerBase.handler()
    async def list_available_quotes(self, ident, msg):
        return MD_AVAILABLE_QUOTES

    @ControllerBase.handler()
    async def modify_subscription(self, ident, msg):
        content = msg["content"]
        ticker_id = content["ticker_id"]
        trades_speed = content["trades_speed"]
        ob_speed = content["order_book_speed"]
        ob_levels = content["order_book_levels"]
        emit_quotes = content["emit_quotes"]
        raw_sub_def = self._subscriptions.get(ticker_id)
        if raw_sub_def:
            g.subscriptions[ticker_id]["raw_sub_def"] = raw_sub_def
        res = {}
        if trades_speed > 0 or emit_quotes:
            res["trades"] = await g.tws_client.req_mkt_data(ticker_id)
        else:
            res["trades"] = g.tws_client.cancel_mkt_data(ticker_id)
        if ob_speed > 0 and ob_levels > 0:
            res["order_book"] = await g.tws_client.req_mkt_depth(ticker_id,
                                                                 ob_levels)
        else:
            res["order_book"] = g.tws_client.cancel_mkt_depth(ticker_id)
        return res

###############################################################################

class TWSReader:

    PUB_ADDR = "inproc://tws-pub"

    def __init__(self, reader):
        self._reader = reader
        self._buf = b""
        self._sock_pub = g.ctx.socket(zmq.PUB)
        self._sock_pub.bind(self.PUB_ADDR)

    async def read_forever(self):
        L.info("TWS: starting reader coroutine ...")
        while True:
            frames = await self.read_one()
            L.debug(pformat(frames))
            handler = self.HANDLERS.get(frames[0])
            if handler:
                try:
                    if inspect.iscoroutinefunction(handler):
                        await handler(self, frames[1:])
                    else:
                        handler(self, frames[1:])
                except Exceptions as e:
                    L.exception("TWS: error when handling message:\n{}\n\n".
                                format(pformat(frames)))
            await self._sock_pub.send_multipart(frames)

    async def read_one(self):
        content_size = None
        content = None
        # if self._buf:
        #     content_size = struct.unpack("!I", self._buf[:4])[0]
        while True:
            # TODO: catch relevant exceptions from readuntil
            self._buf += await self._reader.readuntil(b"\0")
            # print(self._buf)
            if len(self._buf) < 4:
                continue
            if content_size is None:
                content_size = struct.unpack("!I", self._buf[:4])[0]
            if len(self._buf) - 4 >= content_size:
                content = self._buf[4:4+content_size]
                self._buf = self._buf[4+content_size:]
                break
        frames = content.split(b"\0")
        if content[-1] != 0:
            L.warning("TWS: received msg that is not ending with NULL:\n{}"
                      .format(pformat(frames)))
            return frames
        return frames[:-1]

    async def listen_topics_until(self, topics, term_pred, timeout=-1):
        sock = g.ctx.socket(zmq.SUB)
        sock.connect(self.PUB_ADDR)
        for topic in topics:
            sock.subscribe(topic)
        poller = zmq.asyncio.Poller()
        poller.register(sock, zmq.POLLIN)
        tic = time()
        res = []
        while True:
            if timeout >= 0:
                remaining = timeout - ((time() - tic) * 1000)
                if remaining <= 0:
                    break
                evs = await poller.poll(remaining)
                if not evs:
                    break
            frames = await sock.recv_multipart()
            res.append(frames)
            if term_pred(frames):
                break
        sock.close()
        return res

    @staticmethod
    async def emit_update(zm_tid, postfix, data):
        msg_bytes = (" " + json.dumps(data)).encode()
        msg_parts = [zm_tid.encode() + postfix, msg_bytes]
        await g.sock_pub.send_multipart(msg_parts)

    async def handle_tick_price(self, frames):
        itr = iter(frames)
        read = lambda x: tws.decode(x, itr)
        version = read(int)
        ib_tid = read(int)
        zm_tid = g.ib_tid_to_zmtid.get(ib_tid)
        if not zm_tid:
            L.warning("skipping tick_price on ib_tid: {}".format(ib_tid))
            return
        tick_type = read(int)
        price = read(float)
        if version >= 2:
            size = read(int)
        else:
            size = 0
        if version >= 3:
            attrs = read(int)
            # attrs contain info about can_auto_execute and past_limit.
            # What does that data mean?
        sub_def = g.subscriptions[zm_tid]
        raw_sub_def = sub_def["raw_sub_def"]
        if tick_type in [1, 66]:  # bid_price, delayed_bid_price
            if raw_sub_def["emit_quotes"] and size > 0:
                data = {"bid_price": price, "bid_size": size}
                await self.emit_update(zm_tid, b"\x03", data)
        elif tick_type in [2, 67]:  # ask_price, delayed_ask_price
            if raw_sub_def["emit_quotes"] and size > 0:
                data = {"ask_price": price, "ask_size": size}
                await self.emit_update(zm_tid, b"\x03", data)
        elif tick_type in [4, 68]:  # last_price, delayed_last_price
            sub_def["last_price"] = price
            if raw_sub_def["trades_speed"] > 0 and size > 0:
                data = {"price": price, "size": size}
                await self.emit_update(zm_tid, b"\x02", data)
        elif tick_type == 6:  # high
            if raw_sub_def["emit_quotes"]:
                data = {"daily": {"high": price}}
                await self.emit_update(zm_tid, b"\x03", data)
        elif tick_type == 7:  # low
            if raw_sub_def["emit_quotes"]:
                data = {"daily": {"low": price}}
                await self.emit_update(zm_tid, b"\x03", data)
        elif tick_type == 9:  # close_price
            if raw_sub_def["emit_quotes"]:
                data = {"close_price": price}
                await self.emit_update(zm_tid, b"\x03", data)
        elif tick_type == 14:
            if raw_sub_def["emit_quotes"]:
                data = {"daily": {"open": price}}
                await self.emit_update(zm_tid, b"\x03", data)

    async def handle_tick_size(self, frames):
        # skip version on frame 0
        ib_tid = int(frames[1])
        zm_tid = g.ib_tid_to_zmtid.get(ib_tid)
        if not zm_tid:
            L.warning("skipping tick_size on ib_tid: {}".format(ib_tid))
            return
        tick_type = int(frames[2])
        size = int(frames[3])
        sub_def = g.subscriptions[zm_tid]
        raw_sub_def = sub_def["raw_sub_def"]
        if tick_type in [0, 69]:  # bid_size, delayed_bid_size
            if raw_sub_def["emit_quotes"] and size > 0:
                data = {"bid_size": size}
                await self.emit_update(zm_tid, b"\x03", data)
        if tick_type in [3, 70]:  # bid_size, delayed_bid_size
            if raw_sub_def["emit_quotes"] and size > 0:
                data = {"ask_size": size}
                await self.emit_update(zm_tid, b"\x03", data)
        if tick_type in [5, 71]:  # last_size, delayed_last_size
            price = sub_def["last_price"]
            if price is None:
                return
            if raw_sub_def["trades_speed"] > 0 and size > 0:
                data = {"price": price, "size": size}
                await self.emit_update(zm_tid, b"\x02", data)
        if tick_type == 8:  # daily volume
            if raw_sub_def["emit_quotes"]:
                data = {"daily": {"volume": size}}
                await self.emit_update(zm_tid, b"\x03", data)

    def handle_error(self, frames):
        ib_tid = int(frames[1])
        err_code = int(frames[2])
        msg = frames[3].decode()
        s = "TWS: [{}] {}".format(err_code, msg)
        if ib_tid != -1:
            s += " (ib_tid: {})".format(ib_tid)
        L.debug(s)

    def handle_next_valid_id(self, frames):
        g.next_valid_id = int(frames[-1])
        L.debug("TWS: next_valid_id={}".format(g.next_valid_id))

    async def handle_market_depth(self, frames):
        # skip version on frame 0
        ib_tid = int(frames[1])
        zm_tid = g.ib_tid_to_zmtid.get(ib_tid)
        if not zm_tid:
            L.warning("skipping market_depth on ib_tid: {}".format(ib_tid))
            return
        position = int(frames[2])
        # skip operation on frame 3
        side = int(frames[4])
        if side == 0:
            book = "asks"
        elif side == 1:
            book = "bids"
        else:
            L.warning("incorrect side on market_depth_msg: {}".format(frames))
            return
        price = float(frames[5])
        size = int(frames[6])
        data = {book: [{"price": price, "size": size, "position": position}]}
        await self.emit_update(zm_tid, b"\x01", data)

    async def handle_market_depth_l2(self, frames):
        L.warning("market_depth_l2 listener not implemented")

    def handle_managed_accounts(self, frames):
        accounts = [x.decode() for x in frames[1:]]
        g.accounts = accounts
        L.debug("TWS: accounts={}".format(g.accounts))

    HANDLERS = {
        b"1": handle_tick_price,
        b"2": handle_tick_size,
        b"4": handle_error,
        b"9": handle_next_valid_id,
        b"12": handle_market_depth,
        b"13": handle_market_depth_l2,
        b"15": handle_managed_accounts,
    }


class TWSClient:

    def __init__(self, ip, port, client_id=0):
        self._ip = ip
        self._port = port
        self._client_id = client_id
        self._reader = None
        self._writer = None
        self._decoder = None
        self._server_version = None
        self._connection_time = None
        self._optional_capabilities = ""

    def _check_version(self):
        # Version 4 adds support for req_contract_details.
        # Version 37 adds support for ib_contract_id.
        # Version 47 adds support for ib_contract_id on req_mkt_data
        MIN_VERSION = 47
        if self._server_version < MIN_VERSION:
            L.critical("minimum supported tws server version is: {}"
                       .format(MIN_VERSION))
            sys.exit(1)

    async def start(self):
        L.info("TWS: connecting to {}:{} ..."
               .format(self._ip, self._port))
        reader, self._writer = await asyncio.open_connection(self._ip,
                                                             self._port)
        self._reader = TWSReader(reader)
        L.info("TWS: socket connected")
        L.debug("TWS: sending hello ...")
        self._send_hello()
        while True:
            frames = await self._reader.read_one()
            # Sometimes news are delivered before actual response to hello.
            # Just skip the news messages.
            if len(frames) == 2:
                break
        self._server_version = int(frames[0].decode())
        self._connection_time = frames[1].decode()
        L.info("TWS: server_version={}".format(self._server_version))
        self._check_version()
        create_task(self._reader.read_forever())
        L.debug("TWS: sending start_api message ...")
        self._send_start_api()

    def _send_hello(self):
        """Send the initial hello message to the TWS API."""
        v100_prefix = "API\0"
        v100_version = "v{}..{}".format(MIN_CLIENT_VER, MAX_CLIENT_VER)
        msg = tws.make_msg(v100_version)
        msg = v100_prefix.encode("ascii") + msg
        self._writer.write(msg)

    def _send_msg(self, msg_parts):
        """Send msg_parts.
        
        Handles parts of any form."""
        content = "".join([tws.make_field(x) for x in msg_parts])
        msg = tws.make_msg(content)
        self._writer.write(msg)

    def _send_start_api(self):
        """Send the API start message that defines connection parameters."""
        VERSION = 2
        msg_parts = [OUT.START_API, VERSION, self._client_id]
        if self._server_version >= MIN_SERVER_VER_OPTIONAL_CAPABILITIES:
            msg_parts += [self._optional_capabilities]
        self._send_msg(msg_parts)

    def _raise_no_support(self, msg):
        raise InvalidArgumentsException("TWS server ({}) does not support {}"
                                        .format(self._server_version, msg))

    # def _send_req_ids(self, num_ids=1):
    #     # num_ids is deprecated ?
    #     VERSION = 1
    #     parts = [OUT.REQ_IDS, VERSION, num_ids]
    #     self._send_msg(parts)

    def _send_req_contract_details(self, d):
        ib_tid = get_next_ib_tid()
        ver = self._server_version
        err = self._raise_no_support
        if ver < MIN_SERVER_VER_TRADING_CLASS:
            if "trading_class" in d:
                err("trading_class field")
        if ver < MIN_SERVER_VER_LINKING:
            if "primary_exchange" in d:
                err("primary_exchange field")
        VERSION = 8
        parts = [OUT.REQ_CONTRACT_DATA, VERSION]
        parts += [ib_tid]
        parts += [d.get("ib_contract_id", "0")]
        parts += [
            d.get("symbol", ""),
            d.get("sec_type", ""),
            d.get("expiration", ""),
            d.get("strike", "0.0"),
            d.get("right", "")
        ]
        if ver >= 15:
            parts += [d.get("multiplier", "")]
        parts += [d.get("exchange", "")]
        if ver >= MIN_SERVER_VER_PRIMARYEXCH:
            parts += [d.get("primary_exchange", "")]
        elif ver >= MIN_SERVER_VER_LINKING:
            if d.get("primary_exchange") \
                    and d.get("exchange") in ["BEST", "SMART"]:
                parts[-1] += ":" + d["primary_exchange"]
        parts += [d.get("currency", "")]
        parts += [d.get("local_symbol", "")]
        if ver >= MIN_SERVER_VER_TRADING_CLASS:
            parts += [d.get("trading_class", "")]
        if ver >= 31:
            # include_expired is always set to False
            parts += [False]
        parts += [d.get("sec_id_type", ""), d.get("sec_id", "")]
        self._send_msg(parts)
        return ib_tid

    def _decode_contract_data(self, parts, match_rid=None):
        itr = iter(parts)
        read = lambda x: tws.decode(x, itr)
        version = read(int)
        if version >= 3:
            ib_tid = read(int)
        if match_rid is not None and ib_tid != match_rid:
            return None
        d = {}
        d["symbol"] = read(str)
        d["sec_type"] = read(str)
        d["expiration"] = read(str)
        d["strike"] = read(float)
        d["right"] = read(str)
        d["exchange"] = read(str)
        d["currency"] = read(str)
        d["local_symbol"] = read(str)
        d["market_name"] = read(str)
        d["trading_class"] = read(str)
        d["ib_contract_id"] = read(int)
        d["min_tick"] = read(float)
        if self._server_version >= MIN_SERVER_VER_MD_SIZE_MULTIPLIER:
            d["size_multiplier"] = read(int)
        d["multiplier"] = read(str)
        d["order_types"] = read(str).split(",")
        d["valid_exchanges"] = read(str).split(",")
        if version >= 2:
            d["price_magnifier"] = read(int)
        if version >= 4:
            d["under_contract_id"] = read(int)
        if version >= 5:
            d["long_name"] = read(str)
            d["primary_exchange"] = read(str)
        if version >= 6:
            d["contract_month"] = read(str)
            d["industry"] = read(str)
            d["category"] = read(str)
            d["subcategory"] = read(str)
            d["time_zone"] = read(str)
            d["trading_hours"] = read(str)
            d["liquid_hours"] = read(str)
        if version >= 8:
            d["ev_rule"] = read(str)
            d["ev_multiplier"] = read(str)
        if version >= 7:
            num_sec_ids = read(int)
            d["sec_ids"] = sec_ids = {}
            for _ in range(num_sec_ids):
                key = read(str)
                val = read(str)
                sec_ids[key] = val
        if self._server_version >= MIN_SERVER_VER_AGG_GROUP:
            d["agg_group"] = read(int)
        if self._server_version >= MIN_SERVER_VER_UNDERLYING_INFO:
            d["under_symbol"] = read(str)
            d["under_sec_type"] = read(str)
        if self._server_version >= MIN_SERVER_VER_MARKET_RULES:
            d["market_rule_ids"] = read(str)
        if self._server_version >= MIN_SERVER_VER_REAL_EXPIRATION_DATE:
            d["real_expiration_date"] = read(str)
        d = {k: v for k, v in d.items() if v != ""}
        # this must be unique for all the possible tickers
        d["ticker_id"] = "{}@{}".format(d["ib_contract_id"], d["exchange"])
        return d

    # req_contract_details is not available if server_version < 4
    async def req_contract_details(self, d):
        if type(d) is list:
            legs = []
            tid_parts = []
            for leg in d:
                r = await self.req_contract_details(leg)
                assert len(r) == 1, len(r)
                r = r[0]
                r["ratio"] = leg["ratio"]
                r["action"] = leg["action"]
                action_sym = "+" if r["action"] == "BUY" else "-"
                tid_parts.append("{}{}*{}@{}".format(action_sym,
                                                     r["ratio"],
                                                     r["ib_contract_id"],
                                                     r["exchange"]))
                legs.append(r)
            return [{"legs": legs, "ticker_id": "|".join(tid_parts)}]
        zm_tid = d.get("ticker_id")
        if zm_tid:
            r = await self.zmapi_ticker_id_to_ticker(zm_tid, True)
            return await self.req_contract_details(r)
        ib_tid = self._send_req_contract_details(d)
        id_bytes = str(ib_tid).encode()
        L.debug("get_contract_details requested ... (ib_tid: {})"
                .format(ib_tid))
        topics = [b"10", b"52", b"4"]
        def term_pred(x):
            # CONTRACT_DATA_END with matching ib_tid (success)
            if x[0] == b"52" and x[2] == id_bytes:
                return True
            # ERR_MSG with matching ib_tid (failure)
            if x[0] == b"4" and x[2] == id_bytes:
                return True
            return False
        res = await self._reader.listen_topics_until(topics, term_pred)
        if res[-1][0] == b"4":
            parts = res[-1]
            s = "{} ({})".format(parts[4].decode(), parts[3].decode())
            raise InvalidArgumentsException(s)
        # remove req_contract_details_end message
        res = res[:-1]
        # skip msg_id
        data = [self._decode_contract_data(x[1:], ib_tid) for x in res]
        # remove messages that were filtered out
        data = [x for x in data if x is not None]
        return data

    def cancel_mkt_data(self, zm_tid):
        sub_def = g.subscriptions[zm_tid]
        if not sub_def["trades_and_quotes"]:
            return "no change"
        ib_tid = sub_def["ib_tid"]
        VERSION = 1
        parts = [OUT.CANCEL_MKT_DATA, VERSION, ib_tid]
        self._send_msg(parts)
        sub_def["trades_and_quotes"] = False
        sub_def["last_price"] = None
        return "canceled"

    def _send_req_market_data(self, d, ib_tid : int):
        # mkt_data_options = {}  # what is this?
        generic_tick_list = ""  # what is this?
        snapshot = False
        regulatory_snapshot = False  # what is this?
        VERSION = 11
        parts = [OUT.REQ_MKT_DATA, VERSION, ib_tid]
        parts += [d.get("ib_contract_id", "0")]
        parts += [d.get("symbol", "")]
        parts += [d.get("sec_type", "")]
        parts += [""] * 4
        parts += [d.get("exchange", "")]
        parts += [d.get("primary_exchange", "")]
        parts += [d.get("currency", "")]
        parts += [""]
        if self._server_version >= MIN_SERVER_VER_TRADING_CLASS:
            parts += [""]
        if d.get("sec_type") == "BAG":
            parts += [len(d["combo_legs"])]
            for x in d["combo_legs"]:
                parts += [
                    x["ib_contract_id"],
                    x["ratio"],
                    x["action"],
                    x["exchange"]]
        parts += [0]  # under_comp
        parts += [generic_tick_list]
        parts += [snapshot]
        parts += [regulatory_snapshot]
        if self._server_version >= MIN_SERVER_VER_LINKING:
            # mkt_data_options
            parts += [""]
        self._send_msg(parts)

    @lru_cache(maxsize=2**16)
    async def zmapi_ticker_id_to_ticker(self, zm_tid, parse_only=False):
        def conv_single(x):
            spl = x.split("@")
            d = {}
            d["ib_contract_id"] = int(spl[0])
            d["exchange"] = spl[1]
            return d
        spl = zm_tid.split("|")
        if len(spl) == 1:
            return conv_single(zm_tid)
        legs = []
        for s in spl:
            m = re.fullmatch(r"([+-])(\d+)([*])(.*)", s)
            if not m:
                raise InvalidArgumentsException(
                        "error parsing ticker_id")
            op, ratio, mult, ticker_id = m.groups()
            if op == "+":
                action = "BUY"
            elif op == "-":
                action = "SELL"
            else:
                raise InvalidArgumentsException(
                        "invalid operation on ticker_id")
            try:
                ratio = int(ratio)
            except:
                raise InvalidArgumentsException(
                        "invalid/missing ratio on ticker_id")
            if not mult:
                raise InvalidArgumentsException(
                        "'*' missing from ticker_id")
            l = {}
            l.update(conv_single(ticker_id))
            l["ratio"] = ratio
            l["action"] = action
            legs.append(l)
        if parse_only:
            return legs
        d = {}
        d["sec_type"] = "BAG"
        d["combo_legs"] = legs
        # can the info just be filled from the first leg in every situation?
        cd = await self.req_contract_details(legs[0])
        assert len(cd) == 1, len(cd)
        cd = cd[0]
        d["symbol"] = cd["symbol"]
        d["exchange"] = cd["exchange"]
        return d

    async def req_mkt_data(self, zm_tid):
        sub_def = g.subscriptions[zm_tid]
        if sub_def["trades_and_quotes"]:
            return "no change"
        ib_tid = sub_def["ib_tid"]
        g.ib_tid_to_zmtid[ib_tid] = zm_tid
        d = await self.zmapi_ticker_id_to_ticker(zm_tid)
        # pprint(d)
        self._send_req_market_data(d, ib_tid)
        sub_def["trades_and_quotes"] = True
        id_bytes = str(ib_tid).encode()
        topics = [b"81", b"4"]
        def term_pred(x):
            # TICK_REQ_PARAMS with matching ib_tid (success)
            if x[0] == b"81" and x[1] == id_bytes:
                return True
            # ERR_MSG with matching ib_tid (failure)
            if x[0] == b"4" and x[2] == id_bytes:
                return True
            return False
        res = await self._reader.listen_topics_until(topics, term_pred, 10000)
        if not res:
            await self.cancel_mkt_data(zm_tid)
            return "no response, canceled"
        elif res[-1][0] == b"4":
            parts = res[-1]
            s = "{} ({})".format(parts[4].decode(), parts[3].decode())
            sub_def["trades_and_quotes"] = False
            return "*ERROR* {}".format(s)
        return "subscribed"

    def cancel_mkt_depth(self, zm_tid):
        sub_def = g.subscriptions[zm_tid]
        if sub_def["ob_levels"] == 0:
            return "no change"
        ib_tid = sub_def["ib_tid"]
        VERSION = 1
        parts = [OUT.CANCEL_MKT_DEPTH, VERSION, ib_tid]
        self._send_msg(parts)
        sub_def["ob_levels"] = 0
        return "canceled"

    def _send_req_mkt_depth(self, d, ib_tid, ob_levels):
        ver = self._server_version
        VERSION = 5
        parts = [OUT.REQ_MKT_DEPTH, VERSION, ib_tid]
        if ver >= MIN_SERVER_VER_TRADING_CLASS:
            parts += [d.get("ib_contract_id", 0)]
        parts += [d.get("symbol", "")]
        parts += [d.get("sec_type", "")]
        parts += [""] * 4
        parts += [d.get("exchange", "")]
        parts += [d.get("currency", "")]
        parts += [""]
        if ver >= MIN_SERVER_VER_TRADING_CLASS:
            parts += [""]
        parts += [ob_levels]
        if ver >= MIN_SERVER_VER_LINKING:
            # mkt_depth_options  # what is this?
            parts += [""]
        self._send_msg(parts)

    async def req_mkt_depth(self, zm_tid, ob_levels):
        sub_def = g.subscriptions[zm_tid]
        if sub_def["ob_levels"] == ob_levels:
            return "no change"
        elif sub_def["ob_levels"] > 0:
            # already got depth subscription, have to cancel the old one first
            self.cancel_mkt_depth(zm_tid)
        ib_tid = sub_def["ib_tid"]
        g.ib_tid_to_zmtid[ib_tid] = zm_tid
        d = await self.zmapi_ticker_id_to_ticker(zm_tid)
        # pprint(d)
        self._send_req_mkt_depth(d, ib_tid, ob_levels)
        sub_def["ob_levels"] = ob_levels
        id_bytes = str(ib_tid).encode()
        topics = [b"12", b"13", b"4"]
        def term_pred(x):
            # MARKET_DEPTH with matching ib_tid (success)
            if x[0] in (b"12", b"13") and x[2] == id_bytes:
                return True
            # ERR_MSG with matching ib_tid (failure)
            if x[0] == b"4" and x[2] == id_bytes:
                return True
            return False
        res = await self._reader.listen_topics_until(topics, term_pred, 10000)
        if not res:
            await self.cancel_mkt_depth(zm_tid)
            return "no response, canceled"
        elif res[-1][0] == b"4":
            parts = res[-1]
            s = "{} ({})".format(parts[4].decode(), parts[3].decode())
            sub_def["ob_levels"] = 0
            return "*ERROR* {}".format(s)
        return "subscribed"


        

###############################################################################

def parse_args():
    parser = argparse.ArgumentParser(description="ibtws ac/md connector")
    parser.add_argument("md_ctl_addr",
                        help="address to bind to for md ctl socket")
    parser.add_argument("md_pub_addr",
                        help="address to bind to for md pub socket")
    parser.add_argument("ac_ctl_addr",
                        help="address to bind to for ac ctl socket")
    parser.add_argument("ac_pub_addr",
                        help="address to bind to for ac pub socket")
    parser.add_argument("--tws-addr", type=str, default="localhost:7497",
                        help="address to tws/gateway socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    args.tws_ip, args.tws_port = args.tws_addr.split(":")
    args.tws_port = int(args.tws_port)
    return args

def build_logger(logger, propagate=False):
    if type(logger) is str:
        logger = logging.getLogger(name)
    logger.propagate = propagate
    logger.handlers.clear()
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def setup_logging(args):
    global L
    logging.root.setLevel(args.log_level)
    L = build_logger(logging.root)

def init_zmq_sockets(args):
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.md_pub_addr)

def create_subscription_definition():
    ib_tid = get_next_ib_tid()
    return {
            "raw_sub_def": None,
            "trades_and_quotes": False,
            "ob_levels": 0,
            "ib_tid": ib_tid,
            "last_price": None,
    }

def main():
    args = parse_args()
    setup_logging(args)
    init_zmq_sockets(args)
    g.ib_tid_to_zmtid = {}
    # ib_tid to subscription definition
    g.subscriptions = defaultdict(create_subscription_definition)
    g.tws_client = TWSClient(args.tws_ip, args.tws_port)
    g.md_ctl = MDController(g.ctx, args.md_ctl_addr)
    tasks = [
        g.tws_client.start(),
        g.md_ctl.run(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]
    g.loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == "__main__":
    main()

