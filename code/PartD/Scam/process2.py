import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        addr=data["result"][k]["category"]
        print(data["result"][k]["id"], end='')
        print (",",addr,end='')
        print("")