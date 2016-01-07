# Modified disorderBook_main.py for use with the C backend. Work in progress...

import json
import optparse
import subprocess

try:
    from bottle import request, response, route, run
except ImportError:
    from bottle_0_12_9 import request, response, route, run     # copy in our repo

# ----------------------------------------------------------------------------------------

INT_MAX = 2147483647

all_venues = dict()         # dict: venue string ---> dict: stock string ---> PIPE
account_ints = dict()       # dict: account string ---> unique int
current_book_count = 0

auth = dict()

direction_ints = {"buy": 1, "sell": 2}
orderType_ints = {"limit": 1, "market": 2, "fill-or-kill": 3, "fok": 3, "immediate-or-cancel": 4, "ioc": 4}

# ----------------------------------------------------------------------------------------

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

# ----------------------------------------------------------------------------------------


class TooManyBooks (Exception):
    pass


class NoApiKey (Exception):
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
        all_venues[venue][symbol] = subprocess.Popen('disorderCook.exe',
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
    
    b_message = bytes(message, encoding="ascii")
    
    proc.stdin.write(b_message)
    proc.stdin.flush()
    
    return str(proc.stdout.readline(), encoding="ascii")


# ----------------------------------------------------------------------------------------


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
            orderType = data["orderType"]       # FIXME should also accept lowercase ordertype
            direction = data["direction"]
        except KeyError as k:
            response.status = 400
            return MISSING_FIELD
        except TypeError:
            response.status = 400
            return BAD_TYPE
        
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
            account_ints[account] = len(account_ints)

        # Now call the process and get a response...
        
        message = "ORDER {} {} {} {} {}".format(account_ints[account], qty, price, direction_ints[direction], orderType_ints[orderType])
        proc = all_venues[venue][symbol]
        
        raw_response = get_response_from_process(proc, message)
        
        return raw_response     # FIXME
        
    except Exception as e:
        response.status = 500
        return dict_from_exception(e)

# ----------------------------------------------------------------------------------------


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
    
    opts, __ = opt_parser.parse_args()
    
    create_book_if_needed(opts.default_venue, opts.default_symbol)
    
    if opts.accounts_file:
        create_auth_records()
    
    print("disorderBook starting up...\n")
    if not auth:
        print(" -----> Warning: running WITHOUT AUTHENTICATION! <-----\n")
    
    run(host = "127.0.0.1", port = opts.port)
    

if __name__ == "__main__":
    main()
