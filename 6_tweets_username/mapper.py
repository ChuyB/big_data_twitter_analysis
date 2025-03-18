import sys
import json

processed_users = set()  

for line in sys.stdin:
    try:
        tweet = json.loads(line.strip())
        user = tweet.get('user')
        if user:
            username = user.get('username')
            if username:
                lowercase_username = username.lower() 
                if lowercase_username not in processed_users:  
                    processed_users.add(lowercase_username)  
                    for i in range(len(lowercase_username)): #Cambiamos el tamaño del substring que tomaremos
                        for j in range(i + 5, len(lowercase_username) + 1): #tomamos los substrings de cada username
                            substring = lowercase_username[i:j]
                            print(f'{substring}\t1')
    except (json.JSONDecodeError, KeyError):
        continue