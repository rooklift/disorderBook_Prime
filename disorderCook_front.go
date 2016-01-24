package main

import (
    "bufio"
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
)

type PipesStruct struct {
    stdin io.WriteCloser
    stdout io.ReadCloser
}

type OrderStruct struct {
    Symbol      string       `json:"symbol"`
    Stock       string       `json:"stock"`
    Venue       string       `json:"venue"`
    Direction   string       `json:"direction"`
    OrderType   string       `json:"orderType"`
    Account     string       `json:"account"`
    Qty         int32        `json:"qty"`
    Price       int32        `json:"price"`
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

const HEARTBEAT_OK      = `{"ok": true, "error": ""}`
const UNKNOWN_PATH      = `{"ok": false, "error": "Unknown path"}`
const UNKNOWN_VENUE     = `{"ok": false, "error": "Unknown venue"}`
const UNKNOWN_SYMBOL    = `{"ok": false, "error": "Venue is known but symbol is not"}`
const BAD_JSON          = `{"ok": false, "error": "Failed to parse incoming JSON"}`
const URL_MISMATCH      = `{"ok": false, "error": "Venue or symbol in URL did not match that in POST"}`
const MISSING_FIELD     = `{"ok": false, "error": "Missing key or unacceptable value in POST"}`
const UNKNOWN_ORDER     = `{"ok": false, "error": "Unknown order ID"}`
const BAD_ORDER         = `{"ok": false, "error": "Couldn't parse order ID"}`
const AUTH_FAILURE      = `{"ok": false, "error": "Unknown account or wrong API key"}`
const NO_VENUE_HEART    = `{"ok": false, "error": "Venue not up (create it by using it)"}`
const TOO_MANY_BOOKS    = `{"ok": false, "error": "Book limit exceeded! (See command line options)"}`
const NOT_IMPLEMENTED   = `{"ok": false, "error": "Not implemented"}`
const DISABLED          = `{"ok": false, "error": "Disabled or not enabled. (See command line options)"}`

const BUY = 1
const SELL = 2

const LIMIT = 1
const MARKET = 2
const FOK = 3
const IOC = 4

// --------------------------------------------------------------------------------------------

var Books = make(map[string]map[string]PipesStruct)
var AccountInts = make(map[string]int)
var Auth = make(map[string]string)

var AuthMode = false
var BookCount = 0

var C_Process_Lock sync.Mutex

var Options OptionsStruct

var NullPipesStruct PipesStruct     // for comparison purposes

// --------------------------------------------------------------------------------------------

func create_book_if_needed (venue string, symbol string) error {
    
    if Books[venue] == nil {
        
        if BookCount >= Options.MaxBooks {
            return errors.New("Too many books!")
        }
        
        v := make(map[string]PipesStruct)
        Books[venue] = v
    }
    
    if Books[venue][symbol] == NullPipesStruct {
        
        if BookCount >= Options.MaxBooks {
            return errors.New("Too many books!")
        }
        
        command := exec.Command("./disorderCook.exe", venue, symbol)
        i_pipe, _ := command.StdinPipe()
        o_pipe, _ := command.StdoutPipe()
        
        // Should maybe handle errors from the above. FIXME
        
        Books[venue][symbol] = PipesStruct{i_pipe, o_pipe}
        BookCount++
        command.Start()
    }
    return nil
}

func getresponse (command string, venue string, symbol string) string {
    
    response := ""
    
    v := Books[venue]
    if v == nil {
        return UNKNOWN_VENUE
    }
    
    s := Books[venue][symbol]
    if s == NullPipesStruct {
        return UNKNOWN_SYMBOL
    }
    
    C_Process_Lock.Lock()
    
    reader := bufio.NewReader(Books[venue][symbol].stdout)
    fmt.Fprintf(Books[venue][symbol].stdin, command)
    
    for {
        nextpiece, _, _ := reader.ReadLine()
        if string(nextpiece) != "END" {
            response += string(nextpiece) + "\n"
        } else {
            break
        }
    }
    
    C_Process_Lock.Unlock()
    
    return response
}

func handler(writer http.ResponseWriter, request * http.Request) {
    
    var command string
    
    writer.Header().Set("Content-Type", "application/json")
    
    request_api_key := request.Header.Get("X-Starfighter-Authorization")
    if request_api_key == "" {
        request_api_key = request.Header.Get("X-Stockfighter-Authorization")
    }

    path_clean := strings.Trim(request.URL.Path, "\n\r\t /")
    pathlist := strings.Split(path_clean, "/")
    
    // Check for /ob/api/
    
    if pathlist[0] != "ob" || pathlist[1] != "api" {
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
            flag := false
            for v := range Books {
                name = v + " Exchange"
                if flag {
                    fmt.Fprintf(writer, ", ")
                }
                fmt.Fprintf(writer, `{"name": "%s", "state": "open", "venue": "%s"}`, name, v)
                flag = true
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
        flag := false
        for s := range Books[venue] {
            name = s + " Inc"
            if flag {
                fmt.Fprintf(writer, ", ")
            }
            fmt.Fprintf(writer, `{"name": "%s", "symbol": "%s"}`, name, s)
            flag = true
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
            
            res := getresponse("QUOTE\n", venue, symbol)
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
            
            res := getresponse("ORDERBOOK\n", venue, symbol)
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
            
            if AuthMode {           // Do this before the acc_id int is generated
                if Auth[account] != request_api_key || Auth[account] == "" {
                    fmt.Fprintf(writer, AUTH_FAILURE)
                    return
                }
            }
            
            acc_id := AccountInts[account]
            if acc_id == 0 {
                acc_id = len(AccountInts) + 1
                AccountInts[account] = acc_id
            }
            
            res := getresponse("STATUSALL " + strconv.Itoa(acc_id) + "\n" , venue, symbol)
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
            
            res1 := getresponse("__ACC_FROM_ID__ " + strconv.Itoa(id) + "\n", venue, symbol)
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
            
            if request.Method == "DELETE" {
                command = fmt.Sprintf("CANCEL %d\n", id)
            } else {
                command = fmt.Sprintf("STATUS %d\n", id)
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
            
            if raw_order.Venue == "" || raw_order.Symbol == "" || raw_order.Account == "" || raw_order.Qty == 0 || raw_order.Direction == "" || raw_order.OrderType == "" {
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
                if Auth[raw_order.Account] != request_api_key || Auth[raw_order.Account] == "" {
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
            
            command = fmt.Sprintf("ORDER %s %d %d %d %d %d\n", raw_order.Account, acc_id, raw_order.Qty, raw_order.Price, int_direction, int_ordertype)
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
            
            res := getresponse("__SCORES__\n", venue, symbol)
            writer.Header().Set("Content-Type", "text/html")
            fmt.Fprintf(writer, res)
            return
        }
    }
    
    fmt.Fprintf(writer, UNKNOWN_PATH)
    return
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
    portstring := fmt.Sprintf(":%d", Options.Port)
    
    if Options.AccountFilename != "" {
        load_auth()
        AuthMode = true
    }
    
    http.HandleFunc("/", handler)
    http.ListenAndServe(portstring, nil)
}
