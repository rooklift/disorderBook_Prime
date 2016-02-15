package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "io/ioutil"
    "net/http"
    "os"
    "os/exec"
    "strconv"
    "strings"
    "sync"

    "github.com/gorilla/websocket"      // go get github.com/gorilla/websocket
)

type PipesStruct struct {
    stdin io.WriteCloser
    stdout io.ReadCloser
    stderr io.ReadCloser
}

type OrderStruct struct {
    Symbol              string       `json:"symbol"`
    Stock               string       `json:"stock"`
    Venue               string       `json:"venue"`
    Direction           string       `json:"direction"`
    OrderType           string       `json:"orderType"`
    Account             string       `json:"account"`
    Qty                 int32        `json:"qty"`
    Price               int32        `json:"price"`
}

type OptionsStruct struct {
    MaxBooks            int
    Port                int
    WsPort              int
    AccountFilename     string
    DefaultVenue        string
    DefaultSymbol       string
    Excess              bool
}

type WsInfo struct {
    Account             string
    Venue               string
    Symbol              string
    ConnType            int
    MessageChannel      chan string
}

const (
    HEARTBEAT_OK      = `{"ok": true, "error": ""}`
    UNKNOWN_PATH      = `{"ok": false, "error": "Unknown path"}`
    UNKNOWN_VENUE     = `{"ok": false, "error": "Unknown venue"}`
    UNKNOWN_SYMBOL    = `{"ok": false, "error": "Venue is known but symbol is not"}`
    BAD_JSON          = `{"ok": false, "error": "Failed to parse incoming JSON"}`
    URL_MISMATCH      = `{"ok": false, "error": "Venue or symbol in URL did not match that in POST"}`
    MISSING_FIELD     = `{"ok": false, "error": "Missing key or unacceptable value in POST"}`
    UNKNOWN_ORDER     = `{"ok": false, "error": "Unknown order ID"}`
    BAD_ORDER         = `{"ok": false, "error": "Couldn't parse order ID"}`
    AUTH_FAILURE      = `{"ok": false, "error": "Unknown account or wrong API key"}`
    NO_VENUE_HEART    = `{"ok": false, "error": "Venue not up (create it by using it)"}`
    CREATE_BOOK_FAIL  = `{"ok": false, "error": "Couldn't create book! Either: bad name, or too many books exist"}`
    NOT_IMPLEMENTED   = `{"ok": false, "error": "Not implemented"}`
    DISABLED          = `{"ok": false, "error": "Disabled or not enabled. (See command line options)"}`
    BAD_ACCOUNT_NAME  = `{"ok": false, "error": "Bad account name (should be alpha_numeric and sane length)"}`
    BAD_DIRECTION     = `{"ok": false, "error": "Bad direction (should be buy or sell, lowercase)"}`
    BAD_ORDERTYPE     = `{"ok": false, "error": "Bad (unknown) orderType"}`
    BAD_PRICE         = `{"ok": false, "error": "Bad (negative) price"}`
    BAD_QTY           = `{"ok": false, "error": "Bad (non-positive) qty"}`
)

const (
    BUY = 1
    SELL = 2
)

const (
    LIMIT = 1
    MARKET = 2
    FOK = 3
    IOC = 4
)

const (
    TICKER = 1
    EXECUTION = 2
)

const FRONTPAGE = `<html>
<head><title>disorderBook</title></head>
<body><pre>

    disorderBook: unofficial Stockfighter server

    C+Go version
    https://github.com/fohristiwhirl/disorderBook_Prime

    By Amtiskaw (Fohristiwhirl on GitHub)
    With help from cite-reader, Medecau and DanielVF

    Mad props to patio11 for the elegant fundamental design!
    Also inspired by eu90h's Mockfighter


    "WOAH THATS FAST" -- DanielVF
</pre></body></html>`

// -------------------------------------------------------------------------------------------------------

// The following globals are unsafe -- could be touched by multiple goroutines (e.g. web handlers):

var Locks = make(map[string]map[string]*sync.Mutex)     // Annoyingly, the map of backend mutexes is itself unsafe
var Books = make(map[string]map[string]*PipesStruct)
var BookCount = 0
var AccountInts = make(map[string]int)
var WebSocketClients = make([]*WsInfo, 0)

// The following are mutexes for the above:

var Books_Locks_Count_MUTEX sync.RWMutex
var AccountInts_MUTEX sync.RWMutex
var WebSocketClients_MUTEX sync.RWMutex

// The following globals are safe because they are only written to before the various goroutines start:

var Options OptionsStruct
var AuthMode = false
var Auth = make(map[string]string)

// The following globals are safe because they are never "written" to as such:

var Upgrader = websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024}

// -------------------------------------------------------------------------------------------------------

func bad_name(name string) bool {

    if len(name) < 1 || len(name) > 20 {
        return true
    }

    // Disallow all chars except alphanumeric and underscore...

    for _, c := range(name) {
        if c < 48 || (c > 57 && c < 65) || (c > 90 && c < 95) || c == 96 || c > 122 {
            return true
        }
    }

    return false
}

func create_book_if_needed(venue string, symbol string) error {

    if bad_name(venue) || bad_name(symbol) {
        return errors.New("Bad name for a book!")
    }

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    if Books[venue] != nil && Books[venue][symbol] != nil {
        Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock (1) before early return
        return nil
    }
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock (2)

    Books_Locks_Count_MUTEX.Lock()      // <======================================== LOCK for __rw__ with deferred UNLOCK
    defer Books_Locks_Count_MUTEX.Unlock()

    if Books[venue] == nil {

        if BookCount >= Options.MaxBooks {
            return errors.New("Too many books!")
        }

        Books[venue] = make(map[string]*PipesStruct)
        Locks[venue] = make(map[string]*sync.Mutex)
    }

    if BookCount >= Options.MaxBooks {
        return errors.New("Too many books!")
    }

    command := exec.Command("./disorderBook.exe", venue, symbol)
    i_pipe, _ := command.StdinPipe()
    o_pipe, _ := command.StdoutPipe()
    e_pipe, _ := command.StderrPipe()

    // Should maybe handle errors from the above.

    new_pipes_struct := PipesStruct{i_pipe, o_pipe, e_pipe}

    Books[venue][symbol] = &new_pipes_struct
    Locks[venue][symbol] = new(sync.Mutex)      // new() returns a pointer
    BookCount++
    command.Start()
    go ws_controller(venue, symbol)

    return nil
}

func get_response_from_book(command string, venue string, symbol string) string {

    if len(command) == 0 || command[len(command) - 1] != '\n' {
        command = command + "\n"
    }

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    v := Books[venue]
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    if v == nil {
        return UNKNOWN_VENUE
    }

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    s := Books[venue][symbol]
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    if s == nil {
        return UNKNOWN_SYMBOL
    }

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    backend_mutex := Locks[venue][symbol]
    backend_stdin := Books[venue][symbol].stdin
    backend_stdout := Books[venue][symbol].stdout
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    backend_mutex.Lock()

    fmt.Fprintf(backend_stdin, command)

    scanner := bufio.NewScanner(backend_stdout)
    var buffer bytes.Buffer

    for {
        scanner.Scan()
        str_piece := scanner.Text()
        if str_piece != "END" {
            buffer.WriteString(str_piece)
            buffer.WriteByte('\n')
        } else {
            break
        }
    }

    backend_mutex.Unlock()

    return buffer.String()
}

func get_binary_orderbook_to_json(venue string, symbol string) string {

    // See comments in the C code for how the incoming data is formatted

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    v := Books[venue]
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    if v == nil {
        return UNKNOWN_VENUE
    }

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    s := Books[venue][symbol]
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    if s == nil {
        return UNKNOWN_SYMBOL
    }

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    backend_mutex := Locks[venue][symbol]
    backend_stdin := Books[venue][symbol].stdin
    backend_stdout := Books[venue][symbol].stdout
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    backend_mutex.Lock()

    fmt.Fprintf(backend_stdin, "ORDERBOOK_BINARY\n")

    reader := bufio.NewReader(backend_stdout)

    var nextbyte byte
    var qty uint32
    var price uint32
    var commaflag bool

    var buffer bytes.Buffer

    buffer.WriteString("{\n  \"ok\": true,\n  \"venue\": \"")
    buffer.WriteString(venue)
    buffer.WriteString("\",\n  \"symbol\": \"")
    buffer.WriteString(symbol)
    buffer.WriteString("\",\n  \"bids\": [")

    wrote_any_bids := false
    wrote_any_asks := false

    commaflag = false
    for {
        qty = 0

        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte) << 24
        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte) << 16
        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte) << 8
        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte)

        price = 0

        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte) << 24
        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte) << 16
        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte) << 8
        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte)

        if qty != 0 {
            if commaflag {
                buffer.WriteString(",")
            }
            buffer.WriteString("\n    {\"price\": ")
            buffer.WriteString(strconv.FormatUint(uint64(price), 10))
            buffer.WriteString(", \"qty\": ")
            buffer.WriteString(strconv.FormatUint(uint64(qty), 10))
            buffer.WriteString(", \"isBuy\": true}")
            commaflag = true
            wrote_any_bids = true
        } else {
            break
        }
    }

    if wrote_any_bids {
        buffer.WriteString("\n  ")
    }
    buffer.WriteString("],\n  \"asks\": [")

    commaflag = false
    for {
        qty = 0

        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte) << 24
        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte) << 16
        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte) << 8
        nextbyte, _ = reader.ReadByte()
        qty += uint32(nextbyte)

        price = 0

        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte) << 24
        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte) << 16
        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte) << 8
        nextbyte, _ = reader.ReadByte()
        price += uint32(nextbyte)

        if qty != 0 {
            if commaflag {
                buffer.WriteString(",")
            }
            buffer.WriteString("\n    {\"price\": ")
            buffer.WriteString(strconv.FormatUint(uint64(price), 10))
            buffer.WriteString(", \"qty\": ")
            buffer.WriteString(strconv.FormatUint(uint64(qty), 10))
            buffer.WriteString(", \"isBuy\": false}")
            commaflag = true
            wrote_any_asks = true
        } else {
            break
        }
    }

    backend_mutex.Unlock()

    // The above Unlock() has to happen before calling
    // get_response_from_book() for the timestamp since it also locks.

    ts := get_response_from_book("__TIMESTAMP__", venue, symbol)
    ts = strings.Trim(ts, "\n\r\t ")

    if wrote_any_asks {
        buffer.WriteString("\n  ")
    }
    buffer.WriteString("],\n  \"ts\": \"")
    buffer.WriteString(ts)
    buffer.WriteString("\"\n}")

    return buffer.String()
}

func main_handler(writer http.ResponseWriter, request * http.Request) {

    writer.Header().Set("Content-Type", "application/json")     // A few things change this later

    request_api_key := request.Header.Get("X-Starfighter-Authorization")
    if request_api_key == "" {
        request_api_key = request.Header.Get("X-Stockfighter-Authorization")
    }

    path_clean := strings.Trim(request.URL.Path, "\n\r\t /")
    pathlist := strings.Split(path_clean, "/")

    // Welcome message for "/"

    if len(pathlist) == 1 && pathlist[0] == "" {      // The split behaviour means len is never 0
        writer.Header().Set("Content-Type", "text/html")
        fmt.Fprintf(writer, FRONTPAGE)
        return
    }

    // Check for obvious path fails...

    if len(pathlist) < 2 || pathlist[0] != "ob" || pathlist[1] != "api" {
        fmt.Fprintf(writer, UNKNOWN_PATH)
        return
    }

    // General heartbeat...

    if len(pathlist) == 3 {
        if pathlist[2] == "heartbeat" {
            fmt.Fprintf(writer, HEARTBEAT_OK)
            return
        }
    }

    // Venues list...

    if len(pathlist) == 3 {
        if pathlist[2] == "venues" {
            fmt.Fprintf(writer, `{"ok": true, "venues": [`)
            name := ""
            commaflag := false

            Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock

            for v := range Books {
                name = v + " Exchange"
                if commaflag {
                    fmt.Fprintf(writer, ", ")
                }
                fmt.Fprintf(writer, `{"name": "%s", "state": "open", "venue": "%s"}`, name, v)
                commaflag = true
            }

            Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

            fmt.Fprintf(writer, "]}")
            return
        }
    }

    // Venue heartbeat...

    if len(pathlist) == 5 {
        if pathlist[2] == "venues" && pathlist[4] == "heartbeat" {
            venue := pathlist[3]

            Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock

            if Books[venue] == nil {
                fmt.Fprintf(writer, NO_VENUE_HEART)
            } else {
                fmt.Fprintf(writer, `{"ok": true, "venue": "%s"}`, venue)
            }

            Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock
            return
        }
    }

    // Stocks on an exchange...

    list_stocks_flag := false

    if len(pathlist) == 4 {
        if pathlist[2] == "venues" {
            list_stocks_flag = true
        }
    } else if len(pathlist) == 5 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" {
            list_stocks_flag = true
        }
    }

    if list_stocks_flag {
        venue := pathlist[3]

        Books_Locks_Count_MUTEX.RLock()     // <======================================== RLock with deferred RUnlock
        defer Books_Locks_Count_MUTEX.RUnlock()

        if Books[venue] == nil {
            fmt.Fprintf(writer, NO_VENUE_HEART)
            return
        }

        fmt.Fprintf(writer, `{"ok": true, "symbols": [`)
        name := ""
        commaflag := false
        for s := range Books[venue] {
            name = s + " Inc"
            if commaflag {
                fmt.Fprintf(writer, ", ")
            }
            fmt.Fprintf(writer, `{"name": "%s", "symbol": "%s"}`, name, s)
            commaflag = true
        }
        fmt.Fprintf(writer, "]}")
        return
    }

    // Quote...

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "quote" {
            venue := pathlist[3]
            symbol := pathlist[5]

            err := create_book_if_needed(venue, symbol)
            if err != nil {
                fmt.Fprintf(writer, CREATE_BOOK_FAIL)
                return
            }

            res := get_response_from_book("QUOTE", venue, symbol)
            fmt.Fprintf(writer, res)
            return
        }
    }

    // Orderbook...

    if len(pathlist) == 6 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" {
            venue := pathlist[3]
            symbol := pathlist[5]

            err := create_book_if_needed(venue, symbol)
            if err != nil {
                fmt.Fprintf(writer, CREATE_BOOK_FAIL)
                return
            }

            res := get_binary_orderbook_to_json(venue, symbol)
            fmt.Fprintf(writer, res)
            return
        }
    }

    // All orders on a venue...

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "accounts" && pathlist[6] == "orders" {
            fmt.Fprintf(writer, NOT_IMPLEMENTED)
            return
        }
    }

    // All orders on a venue (specific stock)...

    if len(pathlist) == 9 {
        if pathlist[2] == "venues" && pathlist[4] == "accounts" && pathlist[6] == "stocks" && pathlist[8] == "orders" {
            venue := pathlist[3]
            account := pathlist[5]
            symbol := pathlist[7]

            if Options.Excess == false {
                fmt.Fprintf(writer, DISABLED)
                return
            }

            if AuthMode {       // Do this before the acc_id int is generated
                api_key, ok := Auth[account]
                if api_key != request_api_key || ok == false {
                    fmt.Fprintf(writer, AUTH_FAILURE)
                    return
                }
            }

            AccountInts_MUTEX.Lock()            // <---------------------------------------- LOCK for __rw__
            acc_id, ok := AccountInts[account]
            if !ok {
                acc_id = len(AccountInts)
                AccountInts[account] = acc_id
            }
            AccountInts_MUTEX.Unlock()          // <---------------------------------------- UNLOCK

            res := get_response_from_book("STATUSALL " + strconv.Itoa(acc_id), venue, symbol)
            fmt.Fprintf(writer, res)
            return
        }
    }

    // Status and cancel...

    if len(pathlist) == 8 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "orders" {
            venue := pathlist[3]
            symbol := pathlist[5]

            id, err := strconv.Atoi(pathlist[7])
            if err != nil {
                fmt.Fprintf(writer, BAD_ORDER)
                return
            }

            res1 := get_response_from_book("__ACC_FROM_ID__ " + strconv.Itoa(id), venue, symbol)
            res1 = strings.Trim(res1, " \t\n\r")
            reply_list := strings.Split(res1, " ")
            err_string, account := reply_list[0], reply_list[1]

            if err_string == "ERROR" {
                fmt.Fprintf(writer, UNKNOWN_ORDER)
                return
            }

            if AuthMode {
                if Auth[account] != request_api_key || Auth[account] == "" {
                    fmt.Fprintf(writer, AUTH_FAILURE)
                    return
                }
            }

            var command string
            if request.Method == "DELETE" {
                command = fmt.Sprintf("CANCEL %d", id)
            } else {
                command = fmt.Sprintf("STATUS %d", id)
            }
            res2 := get_response_from_book(command, venue, symbol)
            fmt.Fprintf(writer, res2)
            return
        }
    }

    // Order placing...

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "orders" && request.Method == "POST" {
            venue := pathlist[3]
            symbol := pathlist[5]

            raw_order := OrderStruct{}
            decoder := json.NewDecoder(request.Body)
            err := decoder.Decode(&raw_order)

            if err != nil {
                fmt.Fprintf(writer, BAD_JSON)
                return
            }

            // Accept missing fields that can be determined from URL...
            if raw_order.Venue == "" {
                raw_order.Venue = venue
            }
            if raw_order.Symbol == "" && raw_order.Stock == "" {
                raw_order.Symbol = symbol
            }

            // Accept stock as an alias of symbol...
            if raw_order.Stock != "" {
                raw_order.Symbol = raw_order.Stock
            }

            if raw_order.Venue != venue || raw_order.Symbol != symbol {
                fmt.Fprintf(writer, URL_MISMATCH)
                return
            }

            if raw_order.Venue == "" || raw_order.Symbol == "" || raw_order.Account == "" || raw_order.Direction == "" || raw_order.OrderType == "" {
                fmt.Fprintf(writer, MISSING_FIELD)
                return
            }

            if raw_order.Price < 0 {
                fmt.Fprintf(writer, BAD_PRICE)
                return
            }

            if raw_order.Qty < 1 {
                fmt.Fprintf(writer, BAD_QTY)
                return
            }

            if bad_name(raw_order.Account) {
                fmt.Fprintf(writer, BAD_ACCOUNT_NAME)
                return
            }

            int_ordertype := 0
            switch raw_order.OrderType {
                case "ioc":
                    int_ordertype = IOC
                case "immediate-or-cancel":
                    int_ordertype = IOC
                case "fok":
                    int_ordertype = FOK
                case "fill-or-kill":
                    int_ordertype = FOK
                case "limit":
                    int_ordertype = LIMIT
                case "market":
                    int_ordertype = MARKET
                default:
                    fmt.Fprintf(writer, BAD_ORDERTYPE)
                    return
            }

            int_direction := 0
            switch raw_order.Direction {
                case "sell":
                    int_direction = SELL
                case "buy":
                    int_direction = BUY
                default:
                    fmt.Fprintf(writer, BAD_DIRECTION)
                    return
            }

            if AuthMode {
                api_key, ok := Auth[raw_order.Account]
                if api_key != request_api_key || ok == false {
                    fmt.Fprintf(writer, AUTH_FAILURE)
                    return
                }
            }

            err = create_book_if_needed(venue, symbol)
            if err != nil {
                fmt.Fprintf(writer, CREATE_BOOK_FAIL)
                return
            }

            // Do the account-ID generation as late as possible so we don't get unused IDs if we return early

            AccountInts_MUTEX.Lock()            // <---------------------------------------- LOCK for __rw__
            acc_id, ok := AccountInts[raw_order.Account]
            if !ok {
                acc_id = len(AccountInts)
                AccountInts[raw_order.Account] = acc_id
            }
            AccountInts_MUTEX.Unlock()            // <-------------------------------------- UNLOCK

            command := fmt.Sprintf("ORDER %s %d %d %d %d %d", raw_order.Account, acc_id, raw_order.Qty, raw_order.Price, int_direction, int_ordertype)
            res := get_response_from_book(command, venue, symbol)
            fmt.Fprintf(writer, res)
            return
        }
    }

    // Scores...

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "scores" {
            venue := pathlist[3]
            symbol := pathlist[5]

            res := get_response_from_book("__SCORES__", venue, symbol)
            writer.Header().Set("Content-Type", "text/html")
            fmt.Fprintf(writer, res)
            return
        }
    }

    fmt.Fprintf(writer, UNKNOWN_PATH)
    return
}

/*
WebSocket strategy:

For each incoming WS connection, the goroutine ws_handler() puts an entry
in a global struct, storing account, venue, and symbol (some of which
are optional). It also stores a channel used for communication.

Each C backend sends messages to stderr. There is one goroutine per
backend -- ws_controller() -- that reads these messages and passes them
on via the channel (only sending to the correct clients).
*/

func append_to_ws_client_list(info_ptr * WsInfo)  {
    WebSocketClients_MUTEX.Lock()       // <---------------------------------------- LOCK for __rw__
    WebSocketClients = append(WebSocketClients, info_ptr)
    fmt.Printf("WebSocket -OPEN- ... Active == %d\n", len(WebSocketClients))
    WebSocketClients_MUTEX.Unlock()     // <---------------------------------------- UNLOCK
}

func ws_handler(writer http.ResponseWriter, request * http.Request) {

    // http://www.gorillatoolkit.org/pkg/websocket

    conn, err := Upgrader.Upgrade(writer, request, nil)
    if err != nil {
        return
    }

    path_clean := strings.Trim(request.URL.Path, "\n\r\t /")
    pathlist := strings.Split(path_clean, "/")

    if len(pathlist) < 3 || pathlist[0] != "ob" || pathlist[1] != "api" || pathlist[2] != "ws" {
        return
    }

    var account string
    var venue string
    var symbol string
    var info WsInfo

    message_channel := make(chan string, 128)        // Dunno what buffer is appropriate

    //ob/api/ws/:trading_account/venues/:venue/tickertape/stocks/:stock
    if len(pathlist) == 9 && pathlist[4] == "venues" && pathlist[6] == "tickertape" && pathlist[7] == "stocks" {
        account = ""
        venue = pathlist[5]
        symbol = pathlist[8]
        info = WsInfo{account, venue, symbol, TICKER, message_channel}
        append_to_ws_client_list(&info)

    //ob/api/ws/:trading_account/venues/:venue/tickertape
    } else if len(pathlist) == 7 && pathlist[4] == "venues" && pathlist[6] == "tickertape" {
        account = ""
        venue = pathlist[5]
        symbol = ""
        info = WsInfo{account, venue, symbol, TICKER, message_channel}
        append_to_ws_client_list(&info)

    //ob/api/ws/:trading_account/venues/:venue/executions/stocks/:symbol
    } else if len(pathlist) == 9 && pathlist[4] == "venues" && pathlist[6] == "executions" && pathlist[7] == "stocks" {
        account = pathlist[3]
        venue = pathlist[5]
        symbol = pathlist[8]
        info = WsInfo{account, venue, symbol, EXECUTION, message_channel}
        append_to_ws_client_list(&info)

    //ob/api/ws/:trading_account/venues/:venue/executions
    } else if len(pathlist) == 7 && pathlist[4] == "venues" && pathlist[6] == "executions" {
        account = pathlist[3]
        venue = pathlist[5]
        symbol = ""
        info = WsInfo{account, venue, symbol, EXECUTION, message_channel}
        append_to_ws_client_list(&info)

    // invalid URL
    } else {
        conn.Close()
        return
    }

    go ws_null_reader(conn, &info)     // This handles reading and discarding incoming messages

    for {
        msg := <- message_channel
        err = conn.WriteMessage(websocket.TextMessage, []byte(msg))
        if err != nil {
            delete_client_from_global_list(&info)
            return      // The function ws_null_reader() will likely close the connection.
        }
    }
}

// See comments above for WebSocket strategy.

func ws_controller(venue string, symbol string) {

    Books_Locks_Count_MUTEX.RLock()     // <---------------------------------------- RLock
    backend_stderr := Books[venue][symbol].stderr
    Books_Locks_Count_MUTEX.RUnlock()   // <---------------------------------------- RUnlock

    // This is the only goroutine reading the stderr. The lock above is needed
    // just to access the pointer that points at the stderr pipe.
    scanner := bufio.NewScanner(backend_stderr)

    for {
        scanner.Scan()
        headers := strings.Split(scanner.Text(), " ")

        var msg_type int

        // The backend sends a header line in format TYPE ACCOUNT VENUE SYMBOL
        // (the last 2 pieces aren't needed as we know what they are already).

        if headers[0] == "TICKER" {
            msg_type = TICKER
        } else if headers[0] == "EXECUTION" {
            msg_type = EXECUTION
        } else {
            msg_type = 0
            fmt.Println("Unknown WS message type received from backend!")
            fmt.Println("Headers: ", headers)
        }

        var buffer bytes.Buffer

        for {
            scanner.Scan()
            str_piece := scanner.Text()
            if str_piece != "END" {
                buffer.WriteString(str_piece)
                buffer.WriteByte('\n')
            } else {
                break
            }
        }

        WebSocketClients_MUTEX.RLock()      // <---------------------------------------- RLock

        for _, client := range WebSocketClients {

            if client.ConnType != msg_type {
                continue
            }
            if client.Account != headers[1] && client.ConnType == EXECUTION {
                continue
            }
            if client.Venue != venue {
                continue
            }
            if client.Symbol != symbol && client.Symbol != "" {
                continue
            }
            select {
                case client.MessageChannel <- buffer.String() :         // Send message unless buffer is full
                default:
            }
        }

        WebSocketClients_MUTEX.RUnlock()    // <---------------------------------------- RUnlock
    }
}

func delete_client_from_global_list(info_ptr * WsInfo) {

    // Does nothing if the client isn't in the list

    WebSocketClients_MUTEX.Lock()           // <---------------------------------------- LOCK for __rw__

    for i, client_ptr := range WebSocketClients {
        if client_ptr == info_ptr {
            // Replace the pointer to this client by the final
            // pointer in the list, then shorten the list by 1.
            WebSocketClients[i] = WebSocketClients[len(WebSocketClients) - 1]
            WebSocketClients = WebSocketClients[:len(WebSocketClients) - 1]
            fmt.Printf("WebSocket CLOSED ... Active == %d\n", len(WebSocketClients))
            break
        }
    }

    WebSocketClients_MUTEX.Unlock()         // <---------------------------------------- UNLOCK
    return
}

// Apparently reading WebSocket messages from clients is mandatory.
// This function also closes connections if needed.

func ws_null_reader(conn * websocket.Conn, info_ptr * WsInfo) {
    for {
        if _, _, err := conn.NextReader(); err != nil {
            delete_client_from_global_list(info_ptr)
            conn.Close()
            return
        }
    }
}

func load_auth() {

    file, err := ioutil.ReadFile(Options.AccountFilename)
    if err != nil {
        fmt.Printf("Couldn't load and parse accounts file.\n\n")
        os.Exit(1)
    }

    var di interface{}
    err = json.Unmarshal(file, &di)
    if err != nil {
        fmt.Printf("Couldn't load and parse accounts file.\n\n")
        os.Exit(1)
    }

    m, ok := di.(map[string]interface{})
    if !ok {
        fmt.Printf("Accounts file didn't seem to be the correct format.\n\n")
        os.Exit(1)
    }

    for acc, apikey := range m {
        switch apikey.(type) {
            case string:
                Auth[acc] = apikey.(string)
        }
    }

    return
}

func main() {

    maxbooksPtr         := flag.Int("maxbooks", 100, "Maximum number of books")
    portPtr             := flag.Int("port", 8000, "Port for web API and WebSockets")
    accountfilenamePtr  := flag.String("accounts", "", "Accounts file for authentication")
    defaultvenuePtr     := flag.String("venue", "TESTEX", "Default venue")
    defaultsymbolPtr    := flag.String("symbol", "FOOBAR", "Default symbol")
    excessPtr           := flag.Bool("excess", false, "Enable commands that can return excessive responses")

    flag.Parse()

    Options = OptionsStruct{    MaxBooks : *maxbooksPtr,
                                    Port : *portPtr,
                         AccountFilename : *accountfilenamePtr,
                            DefaultVenue : *defaultvenuePtr,
                           DefaultSymbol : *defaultsymbolPtr,
                                  Excess : *excessPtr}

    create_book_if_needed(Options.DefaultVenue, Options.DefaultSymbol)

    fmt.Printf("\ndisorderBook (C+Go version) starting up on port %d\n", Options.Port)

    if Options.AccountFilename != "" {
        load_auth()
        AuthMode = true
    } else {
        fmt.Printf("\n-----> Warning: running WITHOUT AUTHENTICATION! <-----\n\n")
    }

    server_string := fmt.Sprintf("127.0.0.1:%d", Options.Port)

    http.HandleFunc("/", main_handler)
    http.HandleFunc("/ob/api/ws/", ws_handler)
    http.ListenAndServe(server_string, nil)
}
