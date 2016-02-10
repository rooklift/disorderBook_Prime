# disorderBook Prime

An implementation of a **[Stockfighter](http://stockfighter.io)** server in C and Go<br>
Written by Stockfighter user Amtiskaw (a.k.a. Fohristiwhirl on GitHub)

## Usage

* Compile `disorderBook.c` and name the executable `disorderBook.exe`
* Compile `disorderBook_front.go` and run it
* Connect your trading bots to &nbsp; **http://127.0.0.1:8000/ob/api/** &nbsp; instead of the normal URL
* WebSockets are at &nbsp; **ws://127.0.0.1:8000/ob/api/ws/**
* Don't use https or wss

## Authentication

There is no authentication by default. If you want authentication, edit `accounts.json` to contain a list of valid users and their API keys and use the command line option `-accounts accounts.json` (then authentication will work in [the same way](https://starfighter.readme.io/docs/api-authentication-authorization) as on the official servers, via "X-Starfighter-Authorization" headers).

## Other features

* Your bots can use whatever accounts, venues, and symbols they like
* New exchanges/stocks are created as needed when someone tries to do something on them
* Some stupid bots [are available](https://github.com/fohristiwhirl/disorderBook/tree/master/bots) to trade against - you must start them (or many copies) manually
* Scores can be accessed at &nbsp; **/ob/api/venues/&lt;venue&gt;/stocks/&lt;symbol&gt;/scores** &nbsp; (accessing this with your bots is cheating though)

## Issues

* Everything persists forever; we will *eventually* run out of RAM
* The timestamps are only accurate to the nearest second
* By default, only accepts connections from localhost

## Thanks

Thanks to patio11, cite-reader, Medecau, DanielVF, eu90h, rjsamson, rami.

## Python version

This is a remake of my [earlier server made with Python](https://github.com/fohristiwhirl/disorderBook), which is slower.
