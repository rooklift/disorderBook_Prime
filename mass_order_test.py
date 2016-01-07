import random
import subprocess
import time

TEST_TIME = 30

proc = subprocess.Popen('disorderCook.exe', shell = False, stdin = subprocess.PIPE, stdout = subprocess.PIPE)

starttime = time.clock()

def get_response_from_process(proc, message):
    assert(isinstance(message, str))

    if not message.endswith("\n"):
        message += "\n"
    
    b_message = bytes(message, encoding="ascii")
    
    proc.stdin.write(b_message)
    proc.stdin.flush()
    
    return str(proc.stdout.readline(), encoding="ascii")

n = 0
while 1:
    n += 1
    price = random.randint(1, 5000)
    qty = random.randint(1, 100)
    direction = random.choice([1, 2])
    orderType = random.choice([1, 1, 1, 1, 2, 3, 4])

    message = "ORDER {} {} {} {} {}".format(0, qty, price, direction, orderType)
    
    raw_response = get_response_from_process(proc, message)
    
    if time.clock() - starttime > TEST_TIME:
        break

print("{} orders placed in {} seconds".format(n, TEST_TIME))
print("= {} per second".format(n // TEST_TIME))

input()
