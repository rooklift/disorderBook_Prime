import random
import subprocess
import time

TEST_TIME = 10

proc = subprocess.Popen(['./disorderCook.exe', "SELLEX", "CATS"], shell = False, stdin = subprocess.PIPE, stdout = subprocess.PIPE)

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

# ----------------------------------------------------------------------

print("Placing many orders...")
all_account_ids = dict()
starttime = time.clock()
n = 0
while 1:
    # print(n)
    n += 1
    price = random.randint(1, 5000)
    qty = random.randint(1, 100)
    direction = random.choice([1, 2])
    orderType = random.choice([1, 1, 1, 1, 2, 3, 4])

    account = "account" + str(n // 1000)
    if account not in all_account_ids:
        all_account_ids[account] = len(all_account_ids)
    acc_id = all_account_ids[account]
    
    message = "ORDER {} {} {} {} {} {}".format(account, acc_id, qty, price, direction, orderType)
    raw_response = get_response_from_process(proc, message)
    # print(raw_response)
    
    # time.sleep(0.01)
    if time.clock() - starttime > TEST_TIME:
        break

print("{} orders placed in {} seconds".format(n, TEST_TIME))
print("= {} per second".format(n // TEST_TIME))

debug_info = get_response_from_process(proc, "__DEBUG_MEMORY__")
print(debug_info)


print("Getting many quotes...")
starttime = time.clock()
n = 0
while 1:
    n += 1
    raw_response = get_response_from_process(proc, "QUOTE")
    # print(raw_response)
    
    # time.sleep(0.01)
    if time.clock() - starttime > TEST_TIME:
        break

print("{} quotes received in {} seconds".format(n, TEST_TIME))
print("= {} per second".format(n // TEST_TIME))

input()
