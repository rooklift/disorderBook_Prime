# disorderCook

*Sigh*. My idea here was to write the backend in C and the frontend (i.e. http handler) in Python, but it turns out the frontend is where the program is spending most of its time.

Anyway, this thing is now functional and near-complete.

I borrowed the level-based book design from [DanielVF's mutex](https://github.com/DanielVF/Mutex).

## Usage

* Compile `disorderCook.c` and name the executable `disorderCook.exe`
* Run `python3 disorderCook_front.py`
* Connect your trading bots to &nbsp; **http://127.0.0.1:8000/ob/api/** &nbsp; instead of the normal URL
* Don't use https

## Features

Most things are the same as the pure Python server, [disorderBook](https://github.com/fohristiwhirl/disorderBook)
