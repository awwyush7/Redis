from storage.storage import Storage
import time

storage = Storage()
storage.add(1,"123",2)
val = storage.get(1)
print(val)

time.sleep(3)
val = storage.get(1)
print(val)
