# disorderCook

This is a rewrite of my **[Stockfighter](http://stockfighter.io)** server **[disorderBook](https://github.com/fohristiwhirl/disorderBook)**, using C for the backend. The frontend is still in Python.

## Usage

* Compile `disorderCook.c` and name the executable `disorderCook.exe`
* Run `python3 disorderCook_front.py`
* Connect your trading bots to &nbsp; **http://127.0.0.1:8000/ob/api/** &nbsp; instead of the normal URL
* Don't use https

## Features

Most things are the same as the pure Python server, [disorderBook](https://github.com/fohristiwhirl/disorderBook). Internally, I borrowed the level-based book design from [DanielVF's mutex](https://github.com/DanielVF/Mutex).
