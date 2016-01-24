# disorderCook

This is a rewrite of my **[Stockfighter](http://stockfighter.io)** server **[disorderBook](https://github.com/fohristiwhirl/disorderBook)**, using C for the backend. For the frontend, there is a choice of Python or Go implementations.

## Usage

* Compile `disorderCook.c` and name the executable `disorderCook.exe`
* Run either of the frontends `disorderCook_front` (Python or Go versions)
* Connect your trading bots to &nbsp; **http://127.0.0.1:8000/ob/api/** &nbsp; instead of the normal URL
* Don't use https

## Requirements

The frontend still requires the Bottle library. If you need it, a copy is [in the other repo](https://github.com/fohristiwhirl/disorderBook/blob/master/bottle_0_12_9.py) (it's a single file, you can put it next to the other files here and it will be imported). Or just `pip install bottle`.

## Features

Most things are the same as the pure Python server, [disorderBook](https://github.com/fohristiwhirl/disorderBook); but WebSockets are missing. Internally, I borrowed the level-based book design from [DanielVF's mutex](https://github.com/DanielVF/Mutex).
