# disorderBook Prime

* This is a rewrite of my **[Stockfighter](http://stockfighter.io)** server **[disorderBook](https://github.com/fohristiwhirl/disorderBook)**, using C for the backend
* The frontend is now written in Go

## Usage

* Compile `disorderBook.c` and name the executable `disorderBook.exe`
* Compile `disorderBook_front.go` and run it
* Connect your trading bots to &nbsp; **http://127.0.0.1:8000/ob/api/** &nbsp; instead of the normal URL
* WebSockets are at &nbsp; **ws://127.0.0.1:8000/ob/api/ws/**
* Don't use https or wss

## Features

* Most things are the same as the pure Python server, [disorderBook](https://github.com/fohristiwhirl/disorderBook)
* That means they're also basically the same as the official server
* Internally, I borrowed the level-based book design from [DanielVF's Mutex](https://github.com/DanielVF/Mutex)
