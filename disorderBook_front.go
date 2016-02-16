package main

import (
    "bufio"
    "bytes"
    "encoding/json"
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
    "time"

    "github.com/gorilla/websocket"      // go get github.com/gorilla/websocket
)

type PipesStruct struct {
    Stdin io.WriteCloser
    Stdout io.ReadCloser
    Stderr io.ReadCloser
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

type Command struct {
    Venue string
    Symbol string
    Command string
    HubCommand int
    CreateIfNeeded bool
    ResponseChan chan []byte
}

type BookInfo struct {
    Venue string
    Symbol string
}

var HEARTBEAT_OK      = []byte(`{"ok": true, "error": ""}`)
var UNKNOWN_PATH      = []byte(`{"ok": false, "error": "Unknown path"}`)
var UNKNOWN_VENUE     = []byte(`{"ok": false, "error": "Unknown venue"}`)
var UNKNOWN_SYMBOL    = []byte(`{"ok": false, "error": "Venue is known but symbol is not"}`)
var BAD_JSON          = []byte(`{"ok": false, "error": "Failed to parse incoming JSON"}`)
var URL_MISMATCH      = []byte(`{"ok": false, "error": "Venue or symbol in URL did not match that in POST"}`)
var MISSING_FIELD     = []byte(`{"ok": false, "error": "Missing key or unacceptable value in POST"}`)
var UNKNOWN_ORDER     = []byte(`{"ok": false, "error": "Unknown order ID"}`)
var BAD_ORDER         = []byte(`{"ok": false, "error": "Couldn't parse order ID"}`)
var AUTH_FAILURE      = []byte(`{"ok": false, "error": "Unknown account or wrong API key"}`)
var NO_VENUE_HEART    = []byte(`{"ok": false, "error": "Venue not up (create it by using it)"}`)
var BAD_BOOK_NAME     = []byte(`{"ok": false, "error": "Couldn't create book! Bad name for a book!"}`)
var TOO_MANY_BOOKS    = []byte(`{"ok": false, "error": "Couldn't create book! Too many books!"}`)
var NOT_IMPLEMENTED   = []byte(`{"ok": false, "error": "Not implemented"}`)
var DISABLED          = []byte(`{"ok": false, "error": "Disabled or not enabled. (See command line options)"}`)
var BAD_ACCOUNT_NAME  = []byte(`{"ok": false, "error": "Bad account name (should be alpha_numeric and sane length)"}`)
var BAD_DIRECTION     = []byte(`{"ok": false, "error": "Bad direction (should be buy or sell, lowercase)"}`)
var BAD_ORDERTYPE     = []byte(`{"ok": false, "error": "Bad (unknown) orderType"}`)
var BAD_PRICE         = []byte(`{"ok": false, "error": "Bad (negative) price"}`)
var BAD_QTY           = []byte(`{"ok": false, "error": "Bad (non-positive) qty"}`)
var MYSTERY_HUB_CMD   = []byte(`{"ok": false, "error": "Hub received unknown hub command"}`)

const (
    VENUES_LIST = 1
    VENUE_HEARTBEAT = 2
    STOCK_LIST = 3
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

var AccountInts = make(map[string]int)
var WebSocketClients = make([]*WsInfo, 0)

// The following are mutexes for the above:

var AccountInts_MUTEX sync.RWMutex
var WebSocketClients_MUTEX sync.RWMutex

// The following globals are safe because they are only written to before the various goroutines start:

var Options OptionsStruct
var AuthMode = false
var Auth = make(map[string]string)

// The following globals are safe because they are never "written" to as such:

var Upgrader = websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024}
var GlobalCommandChan = make(chan Command)

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

func controller(venue string, symbol string, pipes PipesStruct, command_chan chan Command)  {

    // This goroutine controls the stdout and stdin for a single backend.
    // (stderr (for WebSockets) is handled by a different goroutine.)

    for {
        msg := <- command_chan

        command := msg.Command

        if len(command) == 0 || command[len(command) - 1] != '\n' {
            command = command + "\n"
        }

        fmt.Fprintf(pipes.Stdin, command)

        if command == "ORDERBOOK_BINARY\n" {      // This is a special case since the response is binary
            handle_binary_orderbook_response(pipes.Stdout, msg.Venue, msg.Symbol, msg.ResponseChan)
            continue
        }

        scanner := bufio.NewScanner(pipes.Stdout)
        var buffer bytes.Buffer

        for {
            scanner.Scan()
            str_piece := scanner.Bytes()
            if bytes.Equal(str_piece, []byte("END")) {
                break
            } else {
                buffer.Write(str_piece)
                buffer.WriteByte('\n')
            }
        }

        msg.ResponseChan <- buffer.Bytes()
    }
}

func handle_binary_orderbook_response(backend_stdout io.ReadCloser, venue string, symbol string, result_chan chan []byte) {

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

    ts, _ := time.Now().UTC().MarshalJSON()

    if wrote_any_asks {
        buffer.WriteString("\n  ")
    }
    buffer.WriteString("],\n  \"ts\": ")
    buffer.Write(ts)                        // Already has quotes around it
    buffer.WriteString("\n}")

    result_chan <- buffer.Bytes()
    return
}

func handle_hub_command(msg Command, venue_symbol_map map[string]map[string]bool) {
    var buffer bytes.Buffer
    switch msg.HubCommand {

        case VENUES_LIST:

            commaflag := false
            buffer.WriteString("{\n  \"ok\": true,\n  \"venues\": [")
            for v := range venue_symbol_map {
                name := v + " Exchange"
                if commaflag {
                    buffer.WriteString(",")
                }
                line := fmt.Sprintf("\n    {\"name\": \"%s\", \"state\": \"open\", \"venue\": \"%s\"}", name, v)
                buffer.WriteString(line)
                commaflag = true
            }
            buffer.WriteString("\n  ]\n}")

        case VENUE_HEARTBEAT:

            if venue_symbol_map[msg.Venue] == nil {
                buffer.Write(NO_VENUE_HEART)
            } else {
                line := fmt.Sprintf(`{"ok": true, "venue": "%s"}`, msg.Venue)
                buffer.WriteString(line)
            }

        case STOCK_LIST:

            if venue_symbol_map[msg.Venue] == nil {
                buffer.Write(NO_VENUE_HEART)
            } else {
                commaflag := false
                buffer.WriteString("{\n  \"ok\": true,\n  \"symbols\": [")
                for s := range venue_symbol_map[msg.Venue] {
                    name := s + " Inc"
                    if commaflag {
                        buffer.WriteString(",")
                    }
                    line := fmt.Sprintf("\n    {\"name\": \"%s\", \"symbol\": \"%s\"}", name, s)
                    buffer.WriteString(line)
                    commaflag = true
                }
                buffer.WriteString("\n  ]\n}")
            }

        default:

            buffer.Write(MYSTERY_HUB_CMD)
    }

    msg.ResponseChan <- buffer.Bytes()
    return
}

func hub_command_handler(hub_command_chan chan Command, hub_update_chan chan BookInfo) {

    // Some commands aren't dealt with by passing them to a book but rather are queries of global state.
    // This goroutine keeps a copy of the relevant state up-to-date by receiving messages from the hub
    // about new book creation. It thus is able to deal with the commands for such state.

    venue_symbol_map := make(map[string]map[string]bool)   // bool is adequate here

    for {
        select {
            case cmd := <- hub_command_chan:
                handle_hub_command(cmd, venue_symbol_map)

            case update := <- hub_update_chan:
                if venue_symbol_map[update.Venue] == nil {
                    venue_symbol_map[update.Venue] = make(map[string]bool)
                }
                venue_symbol_map[update.Venue][update.Symbol] = true
        }
    }
}

func hub()  {
    books := make(map[string]map[string]chan Command)
    bookcount := 0

    hub_command_chan := make(chan Command)
    hub_update_chan := make(chan BookInfo)

    go hub_command_handler(hub_command_chan, hub_update_chan)

    for {
        msg := <- GlobalCommandChan

        // Check whether the command was to the hub or to the book...

        if msg.HubCommand != 0 {
            hub_command_chan <- msg
            continue
        }

        // Command was a real command to a book...

        venue, symbol := msg.Venue, msg.Symbol

        if msg.CreateIfNeeded == false {
            if books[venue] == nil {
                msg.ResponseChan <- UNKNOWN_VENUE
                continue
            }
            if books[venue][symbol] == nil {
                msg.ResponseChan <- UNKNOWN_SYMBOL
                continue
            }
        }

        // Either the book exists or we need to create it...

        if books[venue] == nil || books[venue][symbol] == nil {     // Short circuits if no venue

            if bad_name(venue) || bad_name(symbol) {
                msg.ResponseChan <- BAD_BOOK_NAME
                continue
            }

            if bookcount >= Options.MaxBooks {
                msg.ResponseChan <- TOO_MANY_BOOKS
                continue
            }

            if books[venue] == nil {
                books[venue] = make(map[string]chan Command)
            }

            new_command_chan := make(chan Command)
            books[venue][symbol] = new_command_chan
            bookcount += 1

            hub_update_chan <- BookInfo{venue, symbol}

            exec_command := exec.Command("./disorderBook.exe", venue, symbol)
            i_pipe, _ := exec_command.StdinPipe()
            o_pipe, _ := exec_command.StdoutPipe()
            e_pipe, _ := exec_command.StderrPipe()

            // Should maybe handle errors from the above.

            new_pipes_struct := PipesStruct{i_pipe, o_pipe, e_pipe}

            exec_command.Start()
            go ws_controller(venue, symbol, e_pipe)
            go controller(venue, symbol, new_pipes_struct, new_command_chan)
            fmt.Printf("Creating %s %s\n", venue, symbol)
        }

        // The book exists...

        books[venue][symbol] <- msg
    }
}

func relay(msg Command, writer http.ResponseWriter) {

    // Send the message to the hub, read the response via a channel,
    // and then send that response to the http client.

    result_chan := make(chan []byte)
    msg.ResponseChan = result_chan
    GlobalCommandChan <- msg
    res := <- result_chan
    writer.Write(res)
    return
}

func main_handler(writer http.ResponseWriter, request * http.Request) {

    writer.Header().Set("Content-Type", "application/json")     // A few things change this later

    request_api_key := request.Header.Get("X-Starfighter-Authorization")
    if request_api_key == "" {
        request_api_key = request.Header.Get("X-Stockfighter-Authorization")
    }

    path_clean := strings.Trim(request.URL.Path, "\n\r\t /")
    pathlist := strings.Split(path_clean, "/")

    // Welcome message for "/" ..................................................................

    if len(pathlist) == 1 && pathlist[0] == "" {      // The split behaviour means len is never 0
        writer.Header().Set("Content-Type", "text/html")
        fmt.Fprintf(writer, FRONTPAGE)
        return
    }

    // Check for obvious path fails..............................................................

    if len(pathlist) < 2 || pathlist[0] != "ob" || pathlist[1] != "api" {
        writer.Write(UNKNOWN_PATH)
        return
    }

    // General heartbeat.........................................................................

    if len(pathlist) == 3 {
        if pathlist[2] == "heartbeat" {
            writer.Write(HEARTBEAT_OK)
            return
        }
    }

    // Venues list...............................................................................

    if len(pathlist) == 3 {
        if pathlist[2] == "venues" {

            msg := Command{
                HubCommand: VENUES_LIST,
            }
            relay(msg, writer)
            return
        }
    }

    // Venue heartbeat...........................................................................

    if len(pathlist) == 5 {
        if pathlist[2] == "venues" && pathlist[4] == "heartbeat" {
            venue := pathlist[3]

            msg := Command{
                Venue: venue,
                HubCommand: VENUE_HEARTBEAT,
            }
            relay(msg, writer)
            return
        }
    }

    // Stocks on an exchange.....................................................................

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

        msg := Command{
            Venue: venue,
            HubCommand: STOCK_LIST,
        }
        relay(msg, writer)
        return
    }

    // Quote.....................................................................................

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "quote" {
            venue := pathlist[3]
            symbol := pathlist[5]

            msg := Command{
                Venue: venue,
                Symbol: symbol,
                Command: "QUOTE",
                CreateIfNeeded: true,
            }
            relay(msg, writer)
            return
        }
    }

    // Orderbook.................................................................................

    if len(pathlist) == 6 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" {
            venue := pathlist[3]
            symbol := pathlist[5]

            msg := Command{
                Venue: venue,
                Symbol: symbol,
                Command: "ORDERBOOK_BINARY",
                CreateIfNeeded: true,
            }
            relay(msg, writer)
            return
        }
    }

    // All orders on a venue.....................................................................

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "accounts" && pathlist[6] == "orders" {
            writer.Write(NOT_IMPLEMENTED)
            return
        }
    }

    // All orders on a venue (specific stock)....................................................

    if len(pathlist) == 9 {
        if pathlist[2] == "venues" && pathlist[4] == "accounts" && pathlist[6] == "stocks" && pathlist[8] == "orders" {
            venue := pathlist[3]
            account := pathlist[5]
            symbol := pathlist[7]

            if Options.Excess == false {
                writer.Write(DISABLED)
                return
            }

            if AuthMode {       // Do this before the acc_id int is generated
                api_key, ok := Auth[account]
                if api_key != request_api_key || ok == false {
                    writer.Write(AUTH_FAILURE)
                    return
                }
            }

            AccountInts_MUTEX.Lock()
            acc_id, ok := AccountInts[account]
            if !ok {
                acc_id = len(AccountInts)
                AccountInts[account] = acc_id
            }
            AccountInts_MUTEX.Unlock()

            msg := Command{
                Venue: venue,
                Symbol: symbol,
                Command: "STATUSALL " + strconv.Itoa(acc_id),
                CreateIfNeeded: true,
            }
            relay(msg, writer)
            return
        }
    }

    // Status and cancel.........................................................................

    if len(pathlist) == 8 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "orders" {
            venue := pathlist[3]
            symbol := pathlist[5]

            id, err := strconv.Atoi(pathlist[7])
            if err != nil {
                writer.Write(BAD_ORDER)
                return
            }

            // In this instance, the web-handler needs to contact the book for info and NOT
            // send that on to the client. Once we have the info we can make the real request.

            command := "__ACC_FROM_ID__ " + strconv.Itoa(id)
            result_chan := make(chan []byte)

            msg := Command{
                ResponseChan: result_chan,
                Venue: venue,
                Symbol: symbol,
                Command: command,
                CreateIfNeeded: false,
            }
            GlobalCommandChan <- msg
            res1 := <- result_chan

            sres1 := string(res1)
            sres1 = strings.Trim(sres1, " \t\n\r")
            reply_list := strings.Split(sres1, " ")
            err_string, account := reply_list[0], reply_list[1]

            if err_string == "ERROR" {
                writer.Write(UNKNOWN_ORDER)
                return
            }

            if AuthMode {
                if Auth[account] != request_api_key || Auth[account] == "" {
                    writer.Write(AUTH_FAILURE)
                    return
                }
            }

            if request.Method == "DELETE" {
                command = fmt.Sprintf("CANCEL %d", id)
            } else {
                command = fmt.Sprintf("STATUS %d", id)
            }

            msg = Command{
                Venue: venue,
                Symbol: symbol,
                Command: command,
                CreateIfNeeded: false,
            }
            relay(msg, writer)
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
                writer.Write(BAD_JSON)
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
                writer.Write(URL_MISMATCH)
                return
            }

            if raw_order.Venue == "" || raw_order.Symbol == "" || raw_order.Account == "" || raw_order.Direction == "" || raw_order.OrderType == "" {
                writer.Write(MISSING_FIELD)
                return
            }

            if raw_order.Price < 0 {
                writer.Write(BAD_PRICE)
                return
            }

            if raw_order.Qty < 1 {
                writer.Write(BAD_QTY)
                return
            }

            if bad_name(raw_order.Account) {
                writer.Write(BAD_ACCOUNT_NAME)
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
                    writer.Write(BAD_ORDERTYPE)
                    return
            }

            int_direction := 0
            switch raw_order.Direction {
                case "sell":
                    int_direction = SELL
                case "buy":
                    int_direction = BUY
                default:
                    writer.Write(BAD_DIRECTION)
                    return
            }

            if AuthMode {
                api_key, ok := Auth[raw_order.Account]
                if api_key != request_api_key || ok == false {
                    writer.Write(AUTH_FAILURE)
                    return
                }
            }

            // Do the account-ID generation as late as possible so we don't get unused IDs if we return early

            AccountInts_MUTEX.Lock()
            acc_id, ok := AccountInts[raw_order.Account]
            if !ok {
                acc_id = len(AccountInts)
                AccountInts[raw_order.Account] = acc_id
            }
            AccountInts_MUTEX.Unlock()

            command := fmt.Sprintf("ORDER %s %d %d %d %d %d", raw_order.Account, acc_id, raw_order.Qty, raw_order.Price, int_direction, int_ordertype)

            msg := Command{
                Venue: venue,
                Symbol: symbol,
                Command: command,
                CreateIfNeeded: true,
            }
            relay(msg, writer)
            return
        }
    }

    // Scores...

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "scores" {
            venue := pathlist[3]
            symbol := pathlist[5]

            writer.Header().Set("Content-Type", "text/html")

            msg := Command{
                Venue: venue,
                Symbol: symbol,
                Command: "__SCORES__",
                CreateIfNeeded: false,
            }
            relay(msg, writer)
            return
        }
    }

    writer.Write(UNKNOWN_PATH)
    return
}

/*
WebSocket strategy:

For each incoming WS connection, the goroutine ws_handler() puts an entry
in a global struct, storing account, venue, and symbol (some of which
are optional). It also stores a channel used for communication.

Each C backend sends messages to stderr. There is one goroutine per
backend -- ws_controller() -- that reads these messages and passes them
on via the channels (only sending to the correct clients).
*/

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

func ws_controller(venue string, symbol string, backend_stderr io.ReadCloser) {

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

        WebSocketClients_MUTEX.RLock()

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

        WebSocketClients_MUTEX.RUnlock()
    }
}

func append_to_ws_client_list(info_ptr * WsInfo)  {

    WebSocketClients_MUTEX.Lock()
    defer WebSocketClients_MUTEX.Unlock()

    WebSocketClients = append(WebSocketClients, info_ptr)
    fmt.Printf("WebSocket -OPEN- ... Active == %d\n", len(WebSocketClients))
    return
}

func delete_client_from_global_list(info_ptr * WsInfo) {

    // Does nothing if the client isn't in the list

    WebSocketClients_MUTEX.Lock()
    defer WebSocketClients_MUTEX.Unlock()

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

    fmt.Printf("\ndisorderBook (C+Go version) starting up on port %d\n", Options.Port)

    if Options.AccountFilename != "" {
        load_auth()
        AuthMode = true
    } else {
        fmt.Printf("\n-----> Warning: running WITHOUT AUTHENTICATION! <-----\n\n")
    }

    go hub()

    // Create the default venue...
    result_chan := make(chan []byte)
    msg := Command{
        ResponseChan: result_chan,
        Venue: Options.DefaultVenue,
        Symbol: Options.DefaultSymbol,
        CreateIfNeeded: true,
        Command: "THIS_DOES_NOT_MATTER",
    }
    GlobalCommandChan <- msg
    <- result_chan              // Need to read a result to prevent deadlock

    server_string := fmt.Sprintf("127.0.0.1:%d", Options.Port)

    http.HandleFunc("/", main_handler)
    http.HandleFunc("/ob/api/ws/", ws_handler)
    http.ListenAndServe(server_string, nil)
}
