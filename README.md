# disorderCook

*Sigh*. My idea here was to write the backend in C and the frontend (i.e. http handler) in Python, but it turns out the frontend is where the program is spending most of its time.

Anyway, this thing is in progress, but it does work somewhat. Limit, IOC, and Market orders are known to work. Getting the orderbook works. Nothing else works at the time of writing.

I borrowed the level-based book design from [DanielVF's mutex](https://github.com/DanielVF/Mutex).
