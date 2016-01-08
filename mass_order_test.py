import random
import subprocess
import time

TEST_TIME = 30

proc = subprocess.Popen(['disorderCook.exe', "SELLEX", "CATS"], shell = False, stdin = subprocess.PIPE, stdout = subprocess.PIPE)

starttime = time.clock()

def get_response_from_process(proc, message):       # MUST MATCH THE REAL THING IN THE FRONTEND, ELSE DEADLOCK
    assert(isinstance(message, str))

    if not message.endswith("\n"):
        message += "\n"
    
    b_message = bytes(message, encoding = "ascii")
    
    proc.stdin.write(b_message)
    proc.stdin.flush()
    
    result = ""
    
    while 1:
        line = str(proc.stdout.readline(), encoding = "ascii")
        if line.strip() == ("END"):         # line ending with \r\n has been observed to happen
            return result
        else:
            result += line

n = 0
while 1:
    print(n)
    n += 1
    price = random.randint(1, 5000)
    qty = random.randint(1, 100)
    direction = random.choice([1, 2])
    orderType = random.choice([1, 1, 1, 1, 2, 3, 4])

    message = "ORDER {} {} {} {} {}".format(0, qty, price, direction, orderType)
    
    raw_response = get_response_from_process(proc, message)
    
    # time.sleep(0.01)
    if time.clock() - starttime > TEST_TIME:
        break

print("{} orders placed in {} seconds".format(n, TEST_TIME))
print("= {} per second".format(n // TEST_TIME))

input()
