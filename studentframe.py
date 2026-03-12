import pandas as pd

data = {
    "StudentID": [101,102,103,104,105,106,107,108,109,110],
    "Name": ["Alice","Bob","Charlie","Diana","Ethan","Fiona","George","Hannah","Ivan","Julia"],
    "Age": [20,21,19,22,20,21,23,19,22,20],
    "Marks": [88,75,92,65,78,85,70,95,60,80],
    "Grade": ["A","B","A","C","B","A","B","A","C","B"]
}

df = pd.DataFrame(data)

print(df)