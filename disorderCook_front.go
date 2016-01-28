package main

import (
    "bufio"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "github.com/gorilla/websocket"      // go get github.com/gorilla/websocket
    "io"
    "io/ioutil"
    "net/http"
    "os"
    "os/exec"
    "strconv"
    "strings"
    "sync"
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
    MessageChannel      chan string
    StillAlive          bool
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
    TOO_MANY_BOOKS    = `{"ok": false, "error": "Book limit exceeded! (See command line options)"}`
    NOT_IMPLEMENTED   = `{"ok": false, "error": "Not implemented"}`
    DISABLED          = `{"ok": false, "error": "Disabled or not enabled. (See command line options)"}`
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

const FRONTPAGE = `<html>
    <head><title>disorderBook</title></head>
    <body><pre>

    disorderBook: unofficial Stockfighter server

    C+Go version
    https://github.com/fohristiwhirl/disorderCook

    By Amtiskaw (Fohristiwhirl on GitHub)
    With help from cite-reader, Medecau and DanielVF

    Mad props to patio11 for the elegant fundamental design!
    Also inspired by eu90h's Mockfighter


    "WOAH THATS FAST" -- DanielVF
    </pre></body></html>`

// --------------------------------------------------------------------------------------------

var Books = make(map[string]map[string]*PipesStruct)
var Locks = make(map[string]map[string]*sync.Mutex)
var AccountInts = make(map[string]int)
var Auth = make(map[string]string)

var AuthMode = false
var BookCount = 0

var Options OptionsStruct

var TickerClients = make([]*WsInfo, 0)
var ExecutionClients = make([]*WsInfo, 0)
var ClientListLock sync.Mutex

var Upgrader = websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize:1024}

// --------------------------------------------------------------------------------------------

func create_book_if_needed (venue string, symbol string) error {

    if Books[venue] == nil {

        if BookCount >= Options.MaxBooks {
            return errors.New("Too many books!")
        }

        Books[venue] = make(map[string]*PipesStruct)
        Locks[venue] = make(map[string]*sync.Mutex)
    }

    if Books[venue][symbol] == nil {

        if BookCount >= Options.MaxBooks {
            return errors.New("Too many books!")
        }

        command := exec.Command("./disorderCook.exe", venue, symbol)
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
    }
    return nil
}

func getresponse (command string, venue string, symbol string) string {

    v := Books[venue]
    if v == nil {
        return UNKNOWN_VENUE
    }

    s := Books[venue][symbol]
    if s == nil {
        return UNKNOWN_SYMBOL
    }

    if len(command) == 0 || command[len(command) - 1] != '\n' {
        command = command + "\n"
    }

    Locks[venue][symbol].Lock()

    reader := bufio.NewReader(Books[venue][symbol].stdout)
    fmt.Fprintf(Books[venue][symbol].stdin, command)

    response := ""
    for {
        nextpiece, _, _ := reader.ReadLine()
        str_piece := strings.Trim(string(nextpiece), "\n\r")
        if str_piece != "END" {
            response += str_piece + "\n"
        } else {
            break
        }
    }

    Locks[venue][symbol].Unlock()

    return response
}

func get_binary_orderbook_to_json (venue string, symbol string) string {

    // See comments in the C code for how the incoming data is formatted

    v := Books[venue]
    if v == nil {
        return UNKNOWN_VENUE
    }

    s := Books[venue][symbol]
    if s == nil {
        return UNKNOWN_SYMBOL
    }

    Locks[venue][symbol].Lock()

    reader := bufio.NewReader(Books[venue][symbol].stdout)
    fmt.Fprintf(Books[venue][symbol].stdin, "ORDERBOOK_BINARY\n")

    var nextbyte byte
    var qty uint32
    var price uint32
    var commaflag bool

    output := make([]byte, 0, 1024)
    output = append(output, `{"ok": true, "venue": "`...)
    output = append(output, venue...)
    output = append(output, `", "symbol": "`...)
    output = append(output, symbol...)
    output = append(output, `", "bids": [`...)

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
                output = append(output, `, `...)
            }
            output = append(output, `{"price": `...)
            output = append(output, strconv.FormatUint(uint64(price), 10)...)
            output = append(output, `, "qty": `...)
            output = append(output, strconv.FormatUint(uint64(qty), 10)...)
            output = append(output, `, "isBuy": true}`...)
            commaflag = true
        } else {
            break
        }
    }

    output = append(output, `], "asks": [`...)

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
                output = append(output, `, `...)
            }
            output = append(output, `{"price": `...)
            output = append(output, strconv.FormatUint(uint64(price), 10)...)
            output = append(output, `, "qty": `...)
            output = append(output, strconv.FormatUint(uint64(qty), 10)...)
            output = append(output, `, "isBuy": false}`...)
            commaflag = true
        } else {
            break
        }
    }

    Locks[venue][symbol].Unlock()

    ts := getresponse("__TIMESTAMP__", venue, symbol)
    ts = strings.Trim(ts, "\n\r\t ")

    output = append(output, `], "ts": "`...)
    output = append(output, ts...)
    output = append(output, `"}`...)

    return string(output[:])
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
            for v := range Books {
                name = v + " Exchange"
                if commaflag {
                    fmt.Fprintf(writer, ", ")
                }
                fmt.Fprintf(writer, `{"name": "%s", "state": "open", "venue": "%s"}`, name, v)
                commaflag = true
            }
            fmt.Fprintf(writer, "]}")
            return
        }
    }

    // Venue heartbeat...

    if len(pathlist) == 5 {
        if pathlist[2] == "venues" && pathlist[4] == "heartbeat" {
            venue := pathlist[3]
            if Books[venue] == nil {
                fmt.Fprintf(writer, NO_VENUE_HEART)
            } else {
                fmt.Fprintf(writer, `{"ok": true, "venue": "%s"}`, venue)
            }
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
                fmt.Fprintf(writer, TOO_MANY_BOOKS)
                return
            }

            res := getresponse("QUOTE", venue, symbol)
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
                fmt.Fprintf(writer, TOO_MANY_BOOKS)
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

            acc_id := AccountInts[account]
            if acc_id == 0 {
                acc_id = len(AccountInts) + 1
                AccountInts[account] = acc_id
            }

            res := getresponse("STATUSALL " + strconv.Itoa(acc_id), venue, symbol)
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

            res1 := getresponse("__ACC_FROM_ID__ " + strconv.Itoa(id), venue, symbol)
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
            res2 := getresponse(command, venue, symbol)
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

            if raw_order.Venue == "" || raw_order.Symbol == "" || raw_order.Account == "" || raw_order.Qty == 0 ||
                    raw_order.Direction == "" || raw_order.OrderType == "" {
                fmt.Fprintf(writer, MISSING_FIELD)
                return
            }

            // FIXME add length checks

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
            }

            int_direction := 0
            switch raw_order.Direction {
                case "sell":
                    int_direction = SELL
                case "buy":
                    int_direction = BUY
            }

            if AuthMode {       // Do this before the acc_id int is generated
                api_key, ok := Auth[raw_order.Account]
                if api_key != request_api_key || ok == false {
                    fmt.Fprintf(writer, AUTH_FAILURE)
                    return
                }
            }

            acc_id := AccountInts[raw_order.Account]
            if acc_id == 0 {
                acc_id = len(AccountInts) + 1
                AccountInts[raw_order.Account] = acc_id
            }

            err = create_book_if_needed(venue, symbol)
            if err != nil {
                fmt.Fprintf(writer, TOO_MANY_BOOKS)
                return
            }

            command := fmt.Sprintf("ORDER %s %d %d %d %d %d", raw_order.Account, acc_id, raw_order.Qty, raw_order.Price, int_direction, int_ordertype)
            res := getresponse(command, venue, symbol)
            fmt.Fprintf(writer, res)
            return
        }
    }

    // Scores...

    if len(pathlist) == 7 {
        if pathlist[2] == "venues" && pathlist[4] == "stocks" && pathlist[6] == "scores" {
            venue := pathlist[3]
            symbol := pathlist[5]

            res := getresponse("__SCORES__", venue, symbol)
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

    message_channel := make(chan string, 32)        // Dunno what buffer is appropriate

    //ob/api/ws/:trading_account/venues/:venue/tickertape/stocks/:stock
    if len(pathlist) == 9 && pathlist[4] == "venues" && pathlist[6] == "tickertape" && pathlist[7] == "stocks" {
        account = ""
        venue = pathlist[5]
        symbol = pathlist[8]

        info = WsInfo{account, venue, symbol, message_channel, true}

        ClientListLock.Lock()
        TickerClients = append(TickerClients, &info)
        ClientListLock.Unlock()

    //ob/api/ws/:trading_account/venues/:venue/tickertape
    } else if len(pathlist) == 7 && pathlist[4] == "venues" && pathlist[6] == "tickertape" {
        account = ""
        venue = pathlist[5]
        symbol = ""

        info = WsInfo{account, venue, symbol, message_channel, true}

        ClientListLock.Lock()
        TickerClients = append(TickerClients, &info)
        ClientListLock.Unlock()

    //ob/api/ws/:trading_account/venues/:venue/executions/stocks/:symbol
    } else if len(pathlist) == 9 && pathlist[4] == "venues" && pathlist[6] == "executions" && pathlist[7] == "stocks" {
        account = pathlist[3]
        venue = pathlist[5]
        symbol = pathlist[8]

        info = WsInfo{account, venue, symbol, message_channel, true}

        ClientListLock.Lock()
        ExecutionClients = append(ExecutionClients, &info)
        ClientListLock.Unlock()

    //ob/api/ws/:trading_account/venues/:venue/executions
    } else if len(pathlist) == 7 && pathlist[4] == "venues" && pathlist[6] == "executions" {
        account = pathlist[3]
        venue = pathlist[5]
        symbol = ""

        info = WsInfo{account, venue, symbol, message_channel, true}

        ClientListLock.Lock()
        ExecutionClients = append(ExecutionClients, &info)
        ClientListLock.Unlock()

    // invalid URL
    } else {
        return
    }

    go ws_null_reader(conn, &info.StillAlive)     // This handles reading and discarding incoming messages

    for {
        msg := <- message_channel
        err = conn.WriteMessage(websocket.TextMessage, []byte(msg))
        if err != nil {

            ClientListLock.Lock()
            info.StillAlive = false
            ClientListLock.Unlock()

            return
        }
    }
}

// See comments above for WebSocket strategy.

func ws_controller(venue string, symbol string) {

    reader := bufio.NewReader(Books[venue][symbol].stderr)

    for {
        raw_headers, _, _ := reader.ReadLine()
        str_header := strings.Trim(string(raw_headers), "\n\r\t /")
        headers := strings.Split(str_header, " ")

        msg_from_stderr := ""
        for {
            nextpiece, _, _ := reader.ReadLine()
            str_piece := strings.Trim(string(nextpiece), "\n\r")
            if str_piece != "END" {
                msg_from_stderr += str_piece + "\n"
            } else {
                break
            }
        }

        ClientListLock.Lock()

        if headers[0] == "TICKER" {
            for _, client := range TickerClients {
                if client.Venue == venue && (client.Symbol == symbol || client.Symbol == "") {
                    if client.StillAlive {
                        client.MessageChannel <- msg_from_stderr
                    }
                }
            }
        }

        if headers[0] == "EXECUTION" {
            for _, client := range ExecutionClients {
                if client.Account == headers[1] && client.Venue == venue && (client.Symbol == symbol || client.Symbol == "") {
                    if client.StillAlive {
                        client.MessageChannel <- msg_from_stderr
                    }
                }
            }
        }

        ClientListLock.Unlock()
    }
}

// Apparently reading WebSocket messages from clients is mandatory...

func ws_null_reader(conn * websocket.Conn, alive_flag * bool) {
    for {
        if _, _, err := conn.NextReader(); err != nil {

            ClientListLock.Lock()
            *alive_flag = false
            ClientListLock.Unlock()

            conn.Close()
            return
        }
    }
}

func load_auth() {
    file, err := ioutil.ReadFile(Options.AccountFilename)
    if err == nil {
        var di interface{}
        err = json.Unmarshal(file, &di)
        if err == nil {
            m := di.(map[string]interface{})
            for acc, apikey := range m {
                switch apikey.(type) {
                    case string:
                        Auth[acc] = apikey.(string)
                }
            }
        }
    }
    if err != nil {
        fmt.Printf("Couldn't load and parse accounts file.\n")
        os.Exit(1)
    }
    return
}

func main() {

    maxbooksPtr         := flag.Int("maxbooks", 100, "Maximum number of books")
    portPtr             := flag.Int("port", 8000, "Port for web API")
    wsportPtr           := flag.Int("wsport", 8001, "Port for WebSockets")
    accountfilenamePtr  := flag.String("accounts", "", "Accounts file for authentication")
    defaultvenuePtr     := flag.String("venue", "TESTEX", "Default venue")
    defaultsymbolPtr    := flag.String("symbol", "FOOBAR", "Default symbol")
    excessPtr           := flag.Bool("excess", false, "Enable commands that can return excessive responses")

    flag.Parse()

    Options = OptionsStruct{    MaxBooks : *maxbooksPtr,
                                    Port : *portPtr,
                                  WsPort : *wsportPtr,
                         AccountFilename : *accountfilenamePtr,
                            DefaultVenue : *defaultvenuePtr,
                           DefaultSymbol : *defaultsymbolPtr,
                                  Excess : *excessPtr}

    create_book_if_needed(Options.DefaultVenue, Options.DefaultSymbol)

    fmt.Printf("disorderBook (C+Go version) starting up on port %d\n", Options.Port)

    if Options.AccountFilename != "" {
        load_auth()
        AuthMode = true
    } else {
        fmt.Printf("\n -----> Warning: running WITHOUT AUTHENTICATION! <-----\n")
    }

    main_server_string := fmt.Sprintf("127.0.0.1:%d", Options.Port)
    server_mux_main := http.NewServeMux()
    server_mux_main.HandleFunc("/", main_handler)
    go func(){http.ListenAndServe(main_server_string, server_mux_main)}()

    ws_server_string := fmt.Sprintf("127.0.0.1:%d", Options.WsPort)
    server_mux_ws := http.NewServeMux()
    server_mux_ws.HandleFunc("/", ws_handler)
    http.ListenAndServe(ws_server_string, server_mux_ws)

}
