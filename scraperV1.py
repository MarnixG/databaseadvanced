import requests
from bs4 import BeautifulSoup
import pprint
import sched
import time
import  pymongo  as mongo
client = mongo.MongoClient("mongodb://127.0.0.1:27017")
bitcoin_db = client["bitcoin"]
col_waardes = local_database["waardes"]

s = sched.scheduler(time.time, time.sleep)
def bitscraper(sc): 
    request = requests.get("https://www.blockchain.com/btc/unconfirmed-transactions")
    soup = BeautifulSoup(request.text, "html.parser")
    tags = soup.findAll('div', attrs={"class": "sc-1g6z4xm-0 hXyplo"})
    hashcodes = []
    onderdelen = []

    scrapervalue = open('highvalue.txt', 'a')


    for tag in tags: 
        hash = tag.findAll('a', attrs={"class": "sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK"})
        for i in hash:
            #print(i.text)
            hashcodes.append(i.text)
    
        attributen = tag.findAll('span', attrs={"class": "sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC"})
    
        for i in attributen:
            onderdelen.append(i.text + " ")


    time = [] 
    btc = [] 
    usd = []
    dictionbtc = {}
    dictionusd = {}

    for i in range(0, len(onderdelen), 3): 
        time.append(onderdelen[i])

    for i in range (1, len(onderdelen), 3):
        temp= onderdelen[i].rstrip("BTC ")
        btc.append(float(temp))

    for i in range(2, len(onderdelen), 3): 
        usd.append(onderdelen[i])

    for i in range(len(hashcodes)):
        dictionbtc[hashcodes[i]] = btc[i]
        dictionusd[hashcodes[i]] = usd[i]

    btc.sort(reverse=True)
    usd.sort(reverse=True)

    for key, value in dictionbtc.items():
        if dictionbtc[key] == btc[0]:
            
            text = "Hash: " + key + " Time: " + time[0] + " BTC value: " + str(value) + " USD value: " +  dictionusd[key]
            scrapervalue.write(text)
            scrapervalue.write("\n")
            hogewaarde = [{"Hash": key , "Time": time[0], "BTC value": str(value), "USD value": dictionusd[key]}]
            x = col_waardes.insert_one(hogewaarde)
            print(x.inserted_id)
    s.enter(60, 1, bitscraper, (sc,))

s.enter(60, 1, bitscraper, (s,))
s.run()
