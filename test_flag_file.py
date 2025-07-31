from datetime import *
import pandas as pd
import os

user = 'Jordan'
message = ' Hello world'
current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

dictionary = {
    "User": [user],
    "Message": [message],
    "Date": [current_date]
}

df = pd.DataFrame(dictionary)

test = os.path.join(os.path.expanduser("~"), "/Documents")
print(test)