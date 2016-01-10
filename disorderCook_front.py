# TODO:
#
# Add quote
# Add status all orders
# Add status all orders one stock

import json
import optparse
import subprocess

try:
    from bottle import request, response, route, run
except ImportError:
    from bottle_0_12_9 import request, response, route, run     # copy in our repo

# -------------------------------------------------------------------------------------------------------------

INT_MAX = 2147483647
MAXORDERS = 2000000000
MAXACCOUNTS = 2048

all_venues = dict()         # dict: venue string ---> dict: stock string ---> PIPE
account_ints = dict()       # dict: account string ---> unique int
current_book_count = 0

auth = dict()

direction_ints = {"buy": 1, "sell": 2}
orderType_ints = {"limit": 1, "market": 2, "fill-or-kill": 3, "fok": 3, "immediate-or-cancel": 4, "ioc": 4}

# -------------------------------------------------------------------------------------------------------------

BAD_JSON = {"ok": False, "error": "Incoming data was not valid JSON"}
BOOK_ERROR = {"ok": False, "error": "Book limit exceeded! (See command line options)"}
NO_AUTH_ERROR = {"ok": False, "error": "Server is in +authentication mode but no API key was received"}
AUTH_FAILURE = {"ok": False, "error": "Unknown account or wrong API key"}
AUTH_WEIRDFAIL = {"ok": False, "error": "Account of stored data had no associated API key (this is impossible)"}
NO_SUCH_ORDER = {"ok": False, "error": "No such order for that Exchange + Symbol combo"}
MISSING_FIELD = {"ok": False, "error": "Incoming POST was missing required field"}
URL_MISMATCH = {"ok": False, "error": "Incoming POST data disagreed with request URL"}
BAD_TYPE = {"ok": False, "error": "A value in the POST had the wrong type"}
BAD_VALUE = {"ok": False, "error": "Illegal value (usually a non-positive number)"}
BAD_NAME = {"ok": False, "error": "Unacceptable length of account, venue, or symbol"}
TOO_MANY_ACCOUNTS = {"ok": False, "error": "Maximum number of accounts exceeded"}
DISABLED = {"ok": False, "error": "Disabled or not enabled. (See command line options)"}

# -------------------------------------------------------------------------------------------------------------


class TooManyBooks (Exception):
    pass


class NoApiKey (Exception):
    pass


class BadName (Exception):
    pass


def dict_from_exception(e):
    di = dict()
    di["ok"] = False
    di["error"] = str(e)
    return di


def create_book_if_needed(venue, symbol):
    global current_book_count
    
    if venue not in all_venues:
        if opts.maxbooks > 0:
            if current_book_count + 1 > opts.maxbooks:
                raise TooManyBooks
        all_venues[venue] = dict()

    if symbol not in all_venues[venue]:
        if opts.maxbooks > 0:
            if current_book_count + 1 > opts.maxbooks:
                raise TooManyBooks
        all_venues[venue][symbol] = subprocess.Popen(['disorderCook.exe', venue, symbol],
                                                     shell = False,
                                                     stdin = subprocess.PIPE,
                                                     stdout = subprocess.PIPE,
                                                    )
        current_book_count += 1


def api_key_from_headers(headers):
    try:
        return headers.get('X-Starfighter-Authorization')
    except:
        try:
            return headers.get('X-Stockfighter-Authorization')
        except:
            raise NoApiKey


def get_response_from_process(proc, message):
    assert(isinstance(message, str))

    if not message.endswith("\n"):
        message += "\n"
    
    b_message = bytes(message, encoding = "ascii")
    
    proc.stdin.write(b_message)
    proc.stdin.flush()
    
    result = ""
    
    while 1:
        line = str(proc.stdout.readline(), encoding = "ascii")
        if line.strip() == ("END"):         # line ending with \r\n has been observed to happen
            return result
        else:
            result += line
        

def validate_names(account = None, venue = None, symbol = None):        # If nothing else, these
    if account is not None:                                             # things should be below
        if not 0 < len(account) < 20:                                   # the C var MAXTOKENSIZE
            raise BadName
    if venue is not None:
        if not 0 < len(venue) < 20:
            raise BadName
    if symbol is not None:
        if not 0 < len(symbol) < 20:
            raise BadName


# -------------------------------------------------------------------------------------------------------------

@route("/ob/api/heartbeat", "GET")
def heartbeat():
    return {"ok": True, "error": ""}

# -------------------------------------------------------------------------------------------------------------
    
@route("/ob/api/venues", "GET")
def venue_list():
    ret = dict()
    ret["ok"] = True
    ret["venues"] = [{"name": v + " Exchange", "venue": v, "state": "open"} for v in all_venues]
    return ret

# -------------------------------------------------------------------------------------------------------------
    
@route("/ob/api/venues/<venue>/heartbeat", "GET")
def venue_heartbeat(venue):
    if venue in all_venues:
        return {"ok": True, "venue": venue}
    else:
        response.status = 404
        return {"ok": False, "error": "Venue {} does not exist (create it by using it)".format(venue)}

# -------------------------------------------------------------------------------------------------------------
        
@route("/ob/api/venues/<venue>", "GET")
@route("/ob/api/venues/<venue>/stocks", "GET")
def stocklist(venue):
    if venue in all_venues:
        return {"ok": True, "symbols": [{"symbol": symbol, "name": symbol + " Inc"} for symbol in all_venues[venue]]}
    else:
        response.status = 404
        return {"ok": False, "error": "Venue {} does not exist (create it by using it)".format(venue)}

# -------------------------------------------------------------------------------------------------------------
        
@route("/ob/api/venues/<venue>/stocks/<symbol>/orders", "POST")
def make_order(venue, symbol):

    try:
        data = str(request.body.read(), encoding="utf-8")
        data = json.loads(data)
    except:
        response.status = 400
        return BAD_JSON

    try:
    
        # Thanks to cite-reader for the following bug-fix:
        # Match behavior of real Stockfighter: recognize both these forms
        
        if "stock" in data:
            symbol_in_data = data["stock"]
        elif "symbol" in data:
            symbol_in_data = data["symbol"]
        else:
            symbol_in_data = symbol
        
        # Note that official SF handles POSTs that lack venue and stock/symbol (using the URL instead)
        
        if "venue" in data:
            venue_in_data = data["venue"]
        else:
            venue_in_data = venue

        # Various types of faulty POST...
        
        if venue_in_data != venue or symbol_in_data != symbol:
            response.status = 400
            return URL_MISMATCH
        
        try:
            account = data["account"]
            price = int(data["price"])
            qty = int(data["qty"])
            if "orderType" in data:
                orderType = data["orderType"]
            else:
                orderType = data["ordertype"]
            direction = data["direction"]
        except KeyError:
            response.status = 400
            return MISSING_FIELD
        except TypeError:
            response.status = 400
            return BAD_TYPE
        
        try:
            validate_names(account, venue, symbol)
        except BadName:
            response.status = 400
            return BAD_NAME
        
        if price < 0 or price > INT_MAX:
            response.status = 400
            return BAD_VALUE
        if qty < 1 or qty > INT_MAX:
            response.status = 400
            return BAD_VALUE
        if direction not in ("buy", "sell"):
            response.status = 400
            return BAD_VALUE
        if orderType not in ("limit", "market", "fill-or-kill", "immediate-or-cancel", "fok", "ioc"):
            response.status = 400
            return BAD_VALUE
        
        try:
            create_book_if_needed(venue, symbol)
        except TooManyBooks:
            response.status = 400
            return BOOK_ERROR
            
        if auth:
        
            try:
                apikey = api_key_from_headers(request.headers)
            except NoApiKey:
                response.status = 401
                return NO_AUTH_ERROR
            
            if account not in auth:
                response.status = 401
                return AUTH_FAILURE

            if auth[account] != apikey:
                response.status = 401
                return AUTH_FAILURE
        
        if account not in account_ints:
            if len(account_ints) < MAXACCOUNTS:
                account_ints[account] = len(account_ints)
            else:
                response.status = 500
                return TOO_MANY_ACCOUNTS

        # Now call the process and get a response...
        
        message = "ORDER {} {} {} {} {} {}".format(account, account_ints[account], qty, price, direction_ints[direction], orderType_ints[orderType])
        proc = all_venues[venue][symbol]
        
        raw_response = get_response_from_process(proc, message)
        
        response.headers["Content-Type"] = "application/json"
        return raw_response
        
    except Exception as e:
        response.status = 500
        return dict_from_exception(e)

# -------------------------------------------------------------------------------------------------------------

@route("/ob/api/venues/<venue>/stocks/<symbol>", "GET")
def orderbook(venue, symbol):

    try:
        validate_names(None, venue, symbol)
    except BadName:
        response.status = 400
        return BAD_NAME

    try:
        create_book_if_needed(venue, symbol)
    except TooManyBooks:
        response.status = 400
        return BOOK_ERROR

    # Now call the process and get a response...
    
    try:
        proc = all_venues[venue][symbol]
        raw_response = get_response_from_process(proc, "ORDERBOOK")
        response.headers["Content-Type"] = "application/json"
        return raw_response
    except Exception as e:
        response.status = 500
        return dict_from_exception(e)

# -------------------------------------------------------------------------------------------------------------

@route("/ob/api/venues/<venue>/stocks/<symbol>/quote", "GET")
def quote(venue, symbol):

    try:
        validate_names(None, venue, symbol)
    except BadName:
        response.status = 400
        return BAD_NAME

    try:
        create_book_if_needed(venue, symbol)
    except TooManyBooks:
        response.status = 400
        return BOOK_ERROR

    # Now call the process and get a response...
    
    try:
        proc = all_venues[venue][symbol]
        raw_response = get_response_from_process(proc, "QUOTE")
        response.headers["Content-Type"] = "application/json"
        return raw_response
    except Exception as e:
        response.status = 500
        return dict_from_exception(e)

# -------------------------------------------------------------------------------------------------------------

@route("/ob/api/venues/<venue>/stocks/<symbol>/orders/<id>", "GET")
def status(venue, symbol, id):
    
    id = int(id)
    
    try:
        validate_names(None, venue, symbol)
    except BadName:
        response.status = 400
        return BAD_NAME
    
    if id < 0 or id > MAXORDERS - 1:
        response.status = 400
        return BAD_VALUE
    
    try:
        create_book_if_needed(venue, symbol)
    except TooManyBooks:
        response.status = 400
        return BOOK_ERROR
    
    try:
        # Authentication step requires querying the backend for account of the order...
        
        proc = all_venues[venue][symbol]

        ls = get_response_from_process(proc, "__ACC_FROM_ID__ {}".format(id)).split()
        
        if ls[0] == "ERROR":
            response.status = 400
            return NO_SUCH_ORDER
        else:
            account = ls[1]

        if auth:
        
            try:
                apikey = api_key_from_headers(request.headers)
            except NoApiKey:
                response.status = 401
                return NO_AUTH_ERROR
        
            if account not in auth:
                response.status = 401
                return AUTH_WEIRDFAIL

            if auth[account] != apikey:
                response.status = 401
                return AUTH_FAILURE
        
        raw_response = get_response_from_process(proc, "STATUS {}".format(id))
        response.headers["Content-Type"] = "application/json"
        return raw_response
    
    except Exception as e:
        response.status = 500
        return dict_from_exception(e)

# -------------------------------------------------------------------------------------------------------------

@route("/ob/api/venues/<venue>/stocks/<symbol>/orders/<id>", "DELETE")
@route("/ob/api/venues/<venue>/stocks/<symbol>/orders/<id>/cancel", "POST")
def cancel(venue, symbol, id):

    id = int(id)
    
    try:
        validate_names(None, venue, symbol)
    except BadName:
        response.status = 400
        return BAD_NAME
    
    if id < 0 or id > MAXORDERS - 1:
        return BAD_VALUE

    try:
        create_book_if_needed(venue, symbol)
    except TooManyBooks:
        response.status = 400
        return BOOK_ERROR
    
    try:
        # Authentication step requires querying the backend for account of the order...
        
        proc = all_venues[venue][symbol]

        ls = get_response_from_process(proc, "__ACC_FROM_ID__ {}".format(id)).split()
        
        if ls[0] == "ERROR":
            response.status = 400
            return NO_SUCH_ORDER
        else:
            account = ls[1]

        if auth:
        
            try:
                apikey = api_key_from_headers(request.headers)
            except NoApiKey:
                response.status = 401
                return NO_AUTH_ERROR
        
            if account not in auth:
                response.status = 401
                return AUTH_WEIRDFAIL

            if auth[account] != apikey:
                response.status = 401
                return AUTH_FAILURE
        
        raw_response = get_response_from_process(proc, "CANCEL {}".format(id))
        response.headers["Content-Type"] = "application/json"
        return raw_response
    
    except Exception as e:
        response.status = 500
        return dict_from_exception(e)

# -------------------------------------------------------------------------------------------------------------


def create_auth_records():
    global auth
    global opts
    
    with open(opts.accounts_file) as infile:
        auth = json.load(infile)


def main():
    global opts
    
    opt_parser = optparse.OptionParser()
    
    opt_parser.add_option(
        "-b", "--maxbooks",
        dest = "maxbooks",
        type = "int",
        help = "Maximum number of books (exchange/ticker combos) [default: %default]")
    opt_parser.set_defaults(maxbooks = 100)
    
    opt_parser.add_option(
        "-v", "--venue",
        dest = "default_venue",
        type = "str",
        help = "Default venue; always exists [default: %default]")
    opt_parser.set_defaults(default_venue = "TESTEX")

    opt_parser.add_option(
        "-s", "--symbol", "--stock",
        dest = "default_symbol",
        type = "str",
        help = "Default symbol; always exists on default venue [default: %default]")
    opt_parser.set_defaults(default_symbol = "FOOBAR")
    
    opt_parser.add_option(
        "-a", "--accounts",
        dest = "accounts_file",
        type = "str",
        help = "File containing JSON dict of account names mapped to their API keys [default: none]")
    opt_parser.set_defaults(accounts_file = "")

    opt_parser.add_option(
        "-p", "--port",
        dest = "port",
        type = "int",
        help = "Port [default: %default]")
    opt_parser.set_defaults(port = 8000)
    
    opt_parser.add_option(
        "-e", "--extra",
        dest   = "extra",
        action = "store_true",
        help   = "Enable commands that can return excessive responses (all orders on venue)")
    opt_parser.set_defaults(extra = False)
    
    opts, __ = opt_parser.parse_args()
    
    create_book_if_needed(opts.default_venue, opts.default_symbol)
    
    if opts.accounts_file:
        create_auth_records()
    
    print("disorderCook starting up...\n")
    if not auth:
        print(" -----> Warning: running WITHOUT AUTHENTICATION! <-----\n")
    
    run(host = "127.0.0.1", port = opts.port)
    

if __name__ == "__main__":
    main()
