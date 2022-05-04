#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 16:21:33 2022

@author: josevans
"""

API_KEY = "yKNbHI1OqmMdlY+uN3REgVXeZ6RKplFzXZLvTteMcgg="
SECRET = "eWyIc+je5cYNFzVE1pjahskHvCYIIMoTgGqwQOEqt7U="

# Authentication

import requests
import hmac
import base64
import hashlib
import datetime
import json
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO, filename='/home/ec2-user/csvFiles/app.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


nonce = 123

rest_url = 'https://v2stgapi.coinflex.com'
host = 'v2stgapi.coinflex.com'

api_key = API_KEY
api_secret = SECRET

startingBalances = pd.DataFrame({'Asset':['FLEX', 'USD'], 'StartingBalance':[475000, 158590.54]})




masterDf = pd.DataFrame(columns=['hashToken', 'status', 'direction', 'quantityTokens', 'startingPrice', 'minPrice', 'maxPrice', 'startingMasterPosition','drift','leverage', 'timestampEntry']) 
    
masterCount = 0
    
masterPosition = 0
    
candleDf = pd.read_csv('/home/ec2-user/csvFiles/ethCandles.csv')

class PrivateAuth:
    
    def __init__(self, api_key, api_secret, rest_url, host):
        self.api_key = api_key
        self.api_secret = api_secret
        self.rest_url = rest_url
        self.host = host
        
    def construct_header(self, nonce, method, endpoint, body=''):
        
        request = requests.Request(method, self.rest_url + endpoint)
        
        ts = datetime.datetime.utcnow().isoformat()
        
        prepared = request.prepare()
        
        if method == 'GET':
        
            split = ''
            if '?' in prepared.path_url:
                split = prepared.path_url.split('?')
                prepared.body = split[1]
            if prepared.body:
                if split:
                    msg_string = f'{ts}\n{nonce}\n{prepared.method}\n{self.host}\n{split[0]}\n{prepared.body}'
                else:
                    msg_string = f'{ts}\n{nonce}\n{prepared.method}\n{self.host}\n{prepared.path_url}\n{prepared.body}'
            else:
                msg_string = f'{ts}\n{nonce}\n{prepared.method}\n{self.host}\n{prepared.path_url}\n'
        else:
             if body:
                 msg_string = '{}\n{}\n{}\n{}\n{}\n{}'.format(ts, nonce, method, self.host, endpoint, body)
             else:
                 msg_string = '{}\n{}\n{}\n{}\n{}\n'.format(ts, nonce, method, self.host, endpoint)

        
        sig = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                msg_string.encode('utf-8'),
                hashlib.sha256
            ).digest()).decode('utf-8')
        header = {'Content-Type': 'application/json', 'AccessKey': self.api_key,
                  'Timestamp': ts, 'Signature': sig, 'Nonce': str(nonce)}
        return header
    
def private_rest(endpoint,method,body=''):
    auth = PrivateAuth(api_key, api_secret, rest_url, host)
    nonce= str(int(datetime.datetime.utcnow().timestamp() * 1000))
    header = auth.construct_header(nonce, method, endpoint, body)
    try:
        try:
            if method == 'GET':
                resp = requests.get(rest_url + endpoint, headers=header, timeout=15)
                time = datetime.datetime.utcnow()
            elif method == 'POST':
                resp = requests.post(rest_url + endpoint, headers=header, data=body, timeout=60)
                time = datetime.datetime.utcnow()
            elif method == 'DELETE':
                resp = requests.delete(rest_url + endpoint, headers=header, data=body, timeout=30)
                time = datetime.datetime.utcnow()
        except KeyboardInterrupt:
            print('interrupted!')
            logging.exception("Exception occurred")
    except:
        resp = None
        time = datetime.datetime.utcnow()
        logging.exception("Exception occurred")
    if resp != None:
        return resp
    else:
        return False,False,time

def get_all_marketCodes():
    
    ts = datetime.datetime.utcnow().isoformat()
    
    method = "/v2/all/markets"

    msg_string = '{}\n{}\n{}\n{}\n{}'.format(ts, nonce, 'GET', host, method)

    sig = base64.b64encode(hmac.new(api_secret.encode('utf-8'), msg_string.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    header = {'Content-Type': 'application/json', 'AccessKey': api_key,
              'Timestamp': ts, 'Signature': sig, 'Nonce': str(nonce)}
    
    resp = requests.get(rest_url + method, headers=header)
    data = resp.json()['data']
    
    marketCodes = [data[i]['marketCode'] for i in range(len(data))]
    
    return marketCodes

def get_spot_price_data():
    
    ts = datetime.datetime.utcnow().isoformat()
    
    method = "/v2/all/markets"

    msg_string = '{}\n{}\n{}\n{}\n{}'.format(ts, nonce, 'GET', host, method)

    sig = base64.b64encode(hmac.new(api_secret.encode('utf-8'), msg_string.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    header = {'Content-Type': 'application/json', 'AccessKey': api_key,
              'Timestamp': ts, 'Signature': sig, 'Nonce': str(nonce)}
    
    resp = requests.get(rest_url + method, headers=header)
    data = resp.json()['data']
    
    dataDf = pd.DataFrame(data)
    
    spotDf = dataDf.loc[dataDf.type == 'SPOT']
    
    return spotDf
    
marketCodes=get_all_marketCodes()

def get_account_info():
    
    r = private_rest('/v2/accountinfo','GET')
    
    return r.json()['data']

print('Account Info:')
print(get_account_info())

def get_price(market_code):
    
    r = private_rest("/v3/markets?marketCode={}".format(market_code),'GET')
    
    try:
    
        return r.json()['data']
    
    except (AttributeError, ValueError):
        logging.exception("Exception occurred")
        time.sleep(1)
        r = private_rest("/v3/markets?marketCode={}".format(market_code),'GET')
        return r.json()['data']

def calc_program_profit(startingFlex, startingUsd):
    
    startingFlex = float(startingFlex)
    startingUsd = float(startingUsd)

    flexPrice = float(get_price("FLEX-USD")[0]['markPrice'])
    
    currentBalanceDf = get_account_balances()
    
    currentFlex = float(currentBalanceDf.loc[currentBalanceDf['instrumentId'] == 'FLEX', 'total'].iloc[0])
    currentFlex -= startingFlex
    currentUsd = float(currentBalanceDf.loc[currentBalanceDf['instrumentId'] == 'USD', 'total'].iloc[0])
    currentUsd -= startingUsd
    
    openPositions = get_positions()
    
    ammProfit = 0
    
    mainAccProfit = 0
    
    if openPositions != None:
        
        for p in openPositions:
            
            mainAccProfit += float(p['positionPnl'])

    
    for acc in masterDf.hashToken:
        
        manual = get_amm_info(acc)[0]
        
        if manual['status'] == 'EXECUTING':
            
            flexIndex = next((index for (index, d) in enumerate(manual['balances']) if d["asset"] == "FLEX"), None)
            usdIndex = next((index for (index, d) in enumerate(manual['balances']) if d["asset"] == "USD"), None)
            flexBorrowIndex = next((index for (index, d) in enumerate(manual['loans']) if d["borrowedAsset"] == "FLEX"), None)
            usdBorrowIndex = next((index for (index, d) in enumerate(manual['loans']) if d["borrowedAsset"] == "USD"), None)        
       
            
            currentFlex += float(manual['balances'][flexIndex]['total'])
            
            currentFlexValue = currentFlex*flexPrice
            
            if len(manual['loans']) != 0:
            
                if flexBorrowIndex != None:
                    borrowedFlex = float(manual['loans'][flexBorrowIndex]['position'])
                else:
                    borrowedFlex = 0
                if usdBorrowIndex != None:
                    borrowedUsdValue = float(manual['loans'][usdBorrowIndex]['borrowedAmount'])
                    flexLoanCollateral = float(manual['loans'][usdBorrowIndex]['position'])
                else:
                    borrowedUsdValue = 0
                    flexLoanCollateral = 0
            else:
                
                borrowedFlex = 0
                borrowedUsdValue = 0
            
            try:
                currentUsd += float(manual['balances'][usdIndex]['total'])
            except TypeError:

                currentUsd += 0
            
            netUsd = currentUsd-borrowedUsdValue
            
            netFlexValue = currentFlexValue - (borrowedFlex*flexPrice)
            
            if len(manual['positions']) > 0:
                
                positionPl = 0
            
                for i in range(len(manual['positions'])):
                
                    positionPl += float(manual['positions'][i]['positionPnl'])

            else:
                
                positionPl = 0
                                   
            ammProfit += netUsd+netFlexValue+positionPl
    
    totalProfit = ammProfit + mainAccProfit
    
    return totalProfit
        
def create_amm(direction, market_code, minPrice, maxPrice, leverage=None, collateralAsset=None, collateralQuantity=None):
        
    print(datetime.datetime.utcnow())
    
    body = json.dumps({
        "leverage": leverage,
        "direction": direction,
        "marketCode": market_code,
        "collateralAsset": collateralAsset,
        "collateralQuantity": collateralQuantity,
        "minPriceBound": minPrice,
        "maxPriceBound": maxPrice
    })
    

    r = private_rest('/v3/AMM/create','POST',body)
    
    try:
        print(r.json())
        logging.info(r.text)
        hash_token = r.json()['data']['hashToken']
    except:
        logging.info(r.text)
        logging.exception("Exception occurred")
        try:
            time.sleep(5)
            print(r.json())
            logging.info(r.text)
            hash_token = r.json()['data']['hashToken']
        except:
            logging.exception("Exception occurred")
            logging.info(r.text)
            amms = get_amms()
            if amms != None:
                hash_token = amms[0]['hashToken']
            else:
                time.sleep(10)
                amms = get_amms()
                hash_token = amms[0]['hashToken']
        
    print('Hash token for AMM is {}'.format(hash_token))
    
    return hash_token

# hash_token = create_amm("BUY", "BTC-USD-SWAP-LIN", "1000", "43000", "44000", leverage="1", collateralAsset="BTC", collateralQuantity="1")

def redeem_amm(hash_token):
    
    print(datetime.datetime.utcnow())
 
    # Required for POST 
    body = json.dumps({
        "hashToken": hash_token,
        "type": "MANUAL",
        "accountId": "31684"
    })
    
    r = private_rest('/v3/AMM/redeem','POST',body)
    try:
        
        if r.json()['data']:
            logging.info(r.text)
            print(r.json())
            return r.json()
            
    except:
        logging.info(r.text)
        logging.exception("Exception occurred")
        print('Redeemed but timeout error')
      
    
# redeem_amm(hash_token)

def get_amm_info(hash_token):
    
    r = private_rest('/v3/AMM?hashToken={}'.format(hash_token), method='GET')
    
    try:
        ammInfo = r.json()['data']
    except:
        logging.exception("Exception occurred")
        time.sleep(1)
        r = private_rest('/v3/AMM?hashToken={}'.format(hash_token), method='GET')
        ammInfo = r.json()['data']

    return ammInfo
 
def get_amm_trades(hash_token, marketCode='ETH-USD-SWAP-LIN', startTime="", endTime=""):
    
    r = private_rest('/v3/AMM/trades?hashToken={}&marketCode={}&limit=500&startTime={}&endTime={}'.format(hash_token, marketCode, startTime, endTime), method='GET')
    
    try:
        tradesDf = pd.DataFrame(r.json()['data'])
    except (AttributeError, ValueError):
        logging.exception("Exception occurred")
        time.sleep(10)
        try:
            r = private_rest('/v3/AMM/trades?hashToken={}&marketCode={}&limit=500&startTime={}&endTime={}'.format(hash_token, marketCode, startTime, endTime), method='GET')
            tradesDf = pd.DataFrame(r.json()['data'])
        except:
            logging.exception("Exception occurred")
            tradesDf = pd.DataFrame()
    
    if len(tradesDf) > 0:

        while len(tradesDf)%500 == 0:
            time.sleep(5)
            second = private_rest('/v3/AMM/trades?hashToken={}&marketCode={}&limit=500&startTime={}&endTime={}'.format(hash_token, marketCode, startTime, tradesDf.lastMatchedAt.min()), method='GET')
            if tradesDf.lastMatchedAt.min() == pd.DataFrame(second.json()['data']).lastMatchedAt.min():
                break
            else:
                tradesDf = tradesDf.append(pd.DataFrame(second.json()['data']))
                tradesDf = tradesDf.drop_duplicates()
    
    
    tradesDf.index = [hash_token]*len(tradesDf)
    
    try:
    
        record =  pd.read_csv('/home/ec2-user/csvFiles/{}_orders.csv'.format(hash_token)).iloc[-1]
        
        tradesDf['startingPrice'] = record.startingPrice
        
        tradesDf['minPrice'] = record.minPrice
        
        tradesDf['maxPrice'] = record.maxPrice
        
        tradesDf['startingMasterPosition'] = masterPosition
        
        tradesDf['drift'] = record.drift
        
        tradesDf['maxPosition'] = masterDf.loc[masterDf.hashToken == hash_token, 'quantityTokens'] * masterDf.loc[masterDf.hashToken == hash_token, 'startingPrice']
    except:
        logging.exception("Exception occurred")
        print('No orders & No trades')
        
    tradesDf.to_csv('/home/ec2-user/csvFiles/{}_trades.csv'.format(hash_token))
    
    return tradesDf

def get_amm_orders(hashToken, minPrice, maxPrice, drift=0):
    
    time.sleep(10)

    r = private_rest('/v3/AMM/orders?hashToken={}'.format(hashToken), method='GET')
     
    ordersDf = pd.DataFrame(r.json()['data'])
    
    ordersDf.index = [hashToken]*len(ordersDf)
    
    ordersDf['startingPrice'] = price
    
    ordersDf['minPrice'] = minPrice
    
    ordersDf['maxPrice'] = maxPrice
    
    ordersDf['drift'] = drift
    
    ordersDf['maxPosition'] = quantityUsd
    
    ordersDf['startingMasterPosition'] = masterPosition
    
    ordersDf.to_csv('/home/ec2-user/csvFiles/{}_orders.csv'.format(hashToken))
    
    return ordersDf

def get_amm_positions(hash_token):
    
    r = private_rest('/v3/AMM/positions?hashToken={}'.format(hash_token), method='GET')
    
    if len(r.json()['data']) > 0:
     
        positionsDf = r.json()['data'][0]['positions']
    
    else:
        
        positionsDf = []
    
    return positionsDf

def get_amms():
    
    r = private_rest('/v2/AMM?status=EXECUTING', method='GET')
    
    if len(r.json()['data']) > 0:
        
        return r.json()['data']
        
    

def get_account_balances():
    
    r = private_rest('/v2/balances', method='GET')
    
    balancesDf = pd.DataFrame(r.json()['data'])
    
    return balancesDf
    
def get_lifetime_profit():
    
    flexPrice = float(get_price("FLEX-USD")[0]['markPrice'])
    
    r = private_rest('/v2/balances', method='GET')
    
    balancesDf = pd.DataFrame(r.json()['data'])
    
    r2 = private_rest('/v3/AMM/hash-token?status=EXECUTING', method='GET') # &marketCode={marketCode}&limit={limit}&startTime={startTime}&endTime={endTime}'
    
    flex = float(balancesDf.loc[balancesDf['instrumentId'] == 'FLEX', 'total'].iloc[0])
    usd = float(balancesDf.loc[balancesDf['instrumentId'] == 'USD', 'total'].iloc[0])
    
    startFlex = startingBalances.loc[startingBalances['Asset'] == 'FLEX', 'StartingBalance'].iloc[0]
    startUsd = startingBalances.loc[startingBalances['Asset'] == 'USD', 'StartingBalance'].iloc[0]
    
    flex -= startFlex
    usd -= startUsd
    
    openPositions = get_positions()
    
    ammProfit = 0
    
    mainAccProfit = 0
    
    if openPositions != None:
        
        for p in openPositions:
            
            mainAccProfit += float(p['positionPnl'])
            
            
    if r2.json()['success'] != False:
        for i in r2.json()['data']:
            
            manual = get_amm_info(i['hashToken'])[0]
            
            flexIndex = next((index for (index, d) in enumerate(manual['balances']) if d["asset"] == "FLEX"), None)
            usdIndex = next((index for (index, d) in enumerate(manual['balances']) if d["asset"] == "USD"), None)
            flexBorrowIndex = next((index for (index, d) in enumerate(manual['loans']) if d["borrowedAsset"] == "FLEX"), None)
            usdBorrowIndex = next((index for (index, d) in enumerate(manual['loans']) if d["borrowedAsset"] == "USD"), None)        
           
            flex += float(manual['balances'][flexIndex]['total'])
            
            if len(manual['loans']) != 0:
            
                if flexBorrowIndex != None:
                    borrowedFlex = float(manual['loans'][flexBorrowIndex]['position'])
                else:
                    borrowedFlex = 0
                if usdBorrowIndex != None:
                    borrowedUsdValue = float(manual['loans'][usdBorrowIndex]['borrowedAmount'])
                    flexLoanCollateral = float(manual['loans'][usdBorrowIndex]['position'])
                else:
                    borrowedUsdValue = 0
                    flexLoanCollateral = 0
            else:
                
                borrowedFlex = 0
                borrowedUsdValue = 0
                flexLoanCollateral=0
            
            try:
                usd += float(manual['balances'][usdIndex]['total'])
            except TypeError:
           
                usd += 0
            
            flex += flexLoanCollateral-borrowedFlex
            
            currentFlexValue = flex*flexPrice
            
            netUsd = usd-borrowedUsdValue
            
            netFlexValue = currentFlexValue - (borrowedFlex*flexPrice)
            
            if len(manual['positions']) > 0:
                
                positionPl = 0
            
                for i in range(len(manual['positions'])):
                
                    positionPl += float(manual['positions'][i]['positionPnl'])

            else:
                
                positionPl = 0
                                   
            ammProfit += netUsd+netFlexValue+positionPl
            totalProfit = ammProfit + mainAccProfit
    else:
        totalProfit = mainAccProfit+(flex*flexPrice)+usd
    
        
    print('Starting Flex Balance: {}.  Starting USD Balance:{}'.format(startFlex, startUsd))
    print('Current Flex Balance: {}.  Current USD Balance:{}'.format(flex+startFlex, usd+startUsd))
    print('Lifetime P&L = ${}'.format(totalProfit))
    
    return totalProfit

def get_positions():
    
    r = private_rest('/v2/positions', 'GET')
    
    try:
    
        return r.json()['data']
    except:
        logging.exception("Exception occurred")
        time.sleep(1)
        r = private_rest('/v2/positions', 'GET')
        return r.json()['data']

def cancel_all_orders(market_code='ETH-USD-SWAP-LIN'):
    
    r = private_rest('/v2/cancel/orders/{}'.format(market_code),'DELETE')
    
    return r.json()['data']

def get_order_status(clientOrderId):
    
    r = private_rest('/v2.1/orders?{}'.format(clientOrderId),'GET')
    
    return r.json()

def get_orders():
    
    r = private_rest('/v2/orders','GET')
    
    return r.json()

def get_trades(marketCode='ETH-USD-SWAP-LIN', limit=""):
    
    r = private_rest('/v2/trades/{}?limit={}'.format(marketCode, limit),'GET')
    
    return r.json()['data']


def get_deposit_hist(asset=""):
    
    r = private_rest("/v3/deposit?startTime={}".format(int((datetime.datetime.utcnow()-datetime.timedelta(days=7)).timestamp()*1000)),'GET')
    
    return r.json()['data']



def post_market_order(clientOrderId, marketCode, side, quantity, orderType, price="", stopPrice=""):

    # Required for POST 

    orders = [
        {
            "clientOrderId": clientOrderId, 
            "marketCode": marketCode, 
            "side": side, 
            "quantity": quantity,
            "orderType": orderType,
            "stopPrice": stopPrice,
            "limitPrice": price
            }
        ]    
    body = json.dumps({
     "recvWindow": 20000,
    "responseType": "FULL",  
    "orders": orders
        })
    
    r = private_rest('/v2/orders/place','POST',body)
    
    resp = r.json()
    
    return resp

def place_stop_order(recvWindow=None, responseType=None, clientOrderId=None, marketCode='ETH-USD-SWAP-LIN',
                    side=None, quantity=None, timeInForce="GTC", orderType="STOP", price=None, stopPrice=None):
        r = private_rest('/v2/orders/place', 'POST',
                          json.dumps({'recvWindow': 200000,
                                      'timestamp': str(int(datetime.datetime.now().timestamp()*1000)),
                                      'responseType': "FULL",
                                      'orders': [{
                                          'clientOrderId': clientOrderId,
                                          'marketCode': marketCode,
                                          'side': side,
                                          'quantity': quantity,
                                          'timeInForce': timeInForce,
                                          'orderType': orderType,
                                          'limitPrice': price,
                                          'stopPrice': stopPrice,
                                      }]
                                      }
                                     ))
        resp = r.json()
        
        return resp


def get_candles(startTime="", marketCode='ETH-USD-SWAP-LIN'):
    
    candleDf = pd.read_csv('/home/ec2-user/csvFiles/ethCandles.csv')
    
    if len(candleDf) == 0:
        
        startingTimestamp = str((datetime.datetime.now()-datetime.timedelta(days=6)).timestamp()*1000)
        
        startTime = round(float(startingTimestamp))-30000000
    
        r = private_rest('/v3/candles?marketCode={}&timeframe={}&limit=500&startTime={}'.format(marketCode, "60s", startTime), 'GET')

        candleDf = pd.DataFrame(r.json()['data'])
        
        while startingTimestamp < candleDf.openedAt.min():
            
            startTime = int(candleDf.openedAt.min())-30000000
            r = private_rest('/v3/candles?marketCode={}&timeframe={}&limit=500&startTime={}&endTime={}'.format(marketCode, "60s", startTime, candleDf.openedAt.min()), 'GET')

            candleDf = candleDf.append(pd.DataFrame(r.json()['data']))
            
    else:
        
        startTime = candleDf.openedAt.max()
        r = private_rest('/v3/candles?marketCode={}&timeframe={}&limit=500&startTime={}'.format(marketCode, "60s", startTime), 'GET')
        
        candleDf = candleDf.append(pd.DataFrame(r.json()['data']))
    
    candleDf.index = list(range(len(candleDf)))
    dict_columns_type = {'high': float,
                    'close': float,
                    'low':float,
                    'openedAt':int
                
                   }
    candleDf = candleDf.astype(dict_columns_type)
    candleDf = candleDf.drop_duplicates().sort_values('openedAt')
       
   
            
    candleDf['TP'] = (candleDf['close'] + candleDf['low'] + candleDf['high'])/3
    candleDf['std'] = candleDf['TP'].rolling(2880).std(ddof=0)
    candleDf['EMA-TP'] = candleDf['TP'].ewm(2880).mean()
    candleDf['BOLU'] = (candleDf['EMA-TP'] + 2*candleDf['std'])-(candleDf.close.rolling(2880).max()-candleDf.close)*(1/2880)
    candleDf['BOLD'] = (candleDf['EMA-TP'] - 2*candleDf['std'])+(candleDf.close-candleDf.close.rolling(2880).min())*(1/2880)
    
    candleDf = candleDf
    
    candleDf.to_csv('/home/ec2-user/csvFiles/ethCandles.csv')
    
    return candleDf

asset = 'ETH-USD-SWAP-LIN'

assetData = get_price(asset)[0]

price = float(assetData['markPrice'])

tickSize = float(assetData['tickSize'])

upperPriceBound = float(assetData['upperPriceBound'])

lowerPriceBound = float(assetData['lowerPriceBound'])

direction = "NEUTRAL"

quantityUsd = 270000

flexPrice = float(get_price("FLEX-USD")[0]['markPrice'])

quantity = quantityUsd/flexPrice

quantityTokens = quantityUsd/price

spread = 10

initialDrift = 0

if spread > 0:
    
    minPrice = round(price-spread+initialDrift, 2)

    maxPrice = round(price+spread+initialDrift, 2)
    
else:
    minPrice = price-10+initialDrift
    
    maxPrice = price+10+initialDrift

leverage = 1

get_lifetime_profit()

print('----------------------')    

weeklyDict = {'6':[[[21,59],[3]]], '0':[[[13,59], [1]]], '4':[[[13,59], [2]]], '3':[[[12,59],[2]],[[23,59],[1]]], '2':[[[12,59], [2]]]}

breakDict = {'10':[[23,59]], '5':[[3,59],[7,59],[14,29],[15,59],[19,59], [21,59]], '2':[[n, 59]for n in range(24)]+[[n, 29]for n in range(24)]}
   
timesList = [[0,n] for n in range (11)]+[[4,0],[8,0],[14,30],[16,0],[20,0],[22,0]]+[[n, 0] for n in range(1,24)]+[[n, 30] for n in range(1,24)]+[[n, 1] for n in range(1,24)]+[[n, 31] for n in range(1,24)]+[[n, 15] for n in range(1,24)]+[[n, 45] for n in range(1,24)]

timeouts = [[59,50],[59,51],[59,52], [59,53],[59,54],[59,55],[59,56],[59,57],[59,58],[59,59],[29,50],[29,51],[29,52],[29,53],[29,54],[29,55],[29,56],[29,57],[29,58],[29,59],[14,50],[14,51],[14,52],[14,53],[14,54],[14,55],[14,56],[14,57],[14,58],[14,59],[44,50],[44,51],[44,52],[44,53],[44,54],[44,55],[44,56],[44,57],[44,58],[44,59]]    


balanceDf = get_account_balances()
    
startingFlex = balanceDf.loc[balanceDf['instrumentId'] == 'FLEX', 'total'].iloc[0]
startingUsd = balanceDf.loc[balanceDf['instrumentId'] == 'USD', 'total'].iloc[0]
    
mainAccPositions = get_positions()
                    
if mainAccPositions is not None:
                    
        ethMainPosIndex = next((index for (index, d) in enumerate(mainAccPositions) if d["instrumentId"] == "ETH-USD-SWAP-LIN"), None)  
                        
        if ethMainPosIndex is not None:
                        
            mainEthPosition = float(mainAccPositions[ethMainPosIndex]['quantity'])
                            
        else:
            mainEthPosition = 0
else:
        mainEthPosition = 0
            
masterPosition = mainEthPosition
    
if len(masterDf) == 0:
        
        if masterPosition != 0:
                        
            usdPosition = masterPosition*price
            drift = -usdPosition/(quantityUsd*5)                
            mid = price + drift*spread                    
            bid = str(mid-spread)
            offer = str(mid+spread)
            
            hash_token = create_amm(direction, asset, bid, offer, str(leverage), collateralAsset="FLEX", collateralQuantity=str(quantity))            
            get_amm_orders(hash_token, bid, offer, drift)
            
            print('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))
            logging.info('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))

            
            masterDf =  masterDf.append(pd.DataFrame({'hashToken':hash_token, 'status':'EXECUTING','direction':direction,'startingPrice':price, 'minPrice':bid, 'maxPrice':offer,'startingMasterPosition':masterPosition, 'drift':drift, 'quantityTokens':quantityUsd/price, 'leverage':str(leverage), 'collateralAsset':"FLEX", 'collateralUsdValue':str(quantity*flexPrice), 'timestampEntry':datetime.datetime.utcnow()}, index= [masterCount]))
                
            print(masterDf.tail().astype(str))
            logging.info(masterDf.tail().astype(str))
                
            masterCount += 1
            
        else:
           
            print('Sending Instruction to Coinflex')
                    
            hash_token = create_amm(direction, asset, str(minPrice), str(maxPrice), str(leverage), collateralAsset="FLEX", collateralQuantity=str(quantity))
            
            get_amm_orders(hash_token, minPrice, maxPrice)
            
            masterDf =  masterDf.append(pd.DataFrame({'hashToken':hash_token, 'status':'EXECUTING','direction':direction,'startingPrice':price, 'minPrice':minPrice, 'maxPrice':maxPrice,'startingMasterPosition':0, 'drift':initialDrift, 'quantityTokens':quantityUsd/price, 'leverage':str(leverage), 'collateralAsset':"FLEX", 'collateralUsdValue':str(quantity*flexPrice), 'timestampEntry':datetime.datetime.utcnow()}, index= [masterCount]))
                
            print(masterDf.tail().astype(str))
            logging.info(masterDf.tail().astype(str))
                
            masterCount += 1
    
count=0
while True:
        
        timeoutMins = 0
        now = datetime.datetime.utcnow()
        
        if str(now.weekday()) in weeklyDict.keys():  
            
            for i in weeklyDict[str(now.weekday())]:
               
              if [now.hour,now.minute] in i:
                      timeoutMins = float(i[1][0])*60
                              
        if timeoutMins == 0:
          for k, v in breakDict.items():  
            if [now.hour, now.minute] in breakDict[k]:
                      
                      timeoutMins = float(k)
        
        executing = masterDf.loc[masterDf.status == 'EXECUTING']   
     
        if len(executing) > 1:
            hashToken = executing.iloc[0].hashToken
            logging.info('DF Error, redeeming {}'.format(hashToken))
            
            if get_amm_info(hashToken)[0]['status'] == 'EXECUTING':
                try:
                    masterDf.loc[masterDf['hashToken'] == hashToken, 'status'] = 'ENDED'
                    redeem_amm(hashToken)
                    # get_amm_trades(hashToken)
                    print(masterDf.tail().astype(str))
                    time.sleep(10)
                            
                except:
                    logging.exception("Exception occurred")
            else:
                masterDf.loc[masterDf['hashToken'] == hashToken, 'status'] = 'ENDED'
             
            
              
        
        for hashToken in masterDf.iloc[-3:].hashToken:   
            
            if get_amm_info(hashToken)[0]['status'] == 'EXECUTING':
                
                if timeoutMins !=0:
                    
                      flexPrice = float(get_price("FLEX-USD")[0]['markPrice'])

                      print('Redeeming {}'.format(hashToken))
                      logging.info('Redeeming {}'.format(hashToken))
                
                      try:
            
                        masterDf.loc[masterDf['hashToken'] == hashToken, 'status'] = 'ENDED'
                        redeem_amm(hashToken)
                        print(masterDf.tail().astype(str))
                        
                        time.sleep(10)
                        
                      except:
                        logging.exception("Exception occurred")
                      
                        
                      positions = get_amm_info(hashToken)[0]['positions']
                
                      assetData = get_price(asset)[0]
            
                      price = float(assetData['markPrice'])
    
                      mainAccPositions = get_positions()
                    
                      if mainAccPositions is not None:
                    
                        ethMainPosIndex = next((index for (index, d) in enumerate(mainAccPositions) if d["instrumentId"] == "ETH-USD-SWAP-LIN"), None)  
                        
                        if ethMainPosIndex is not None:
                        
                            mainEthPosition = float(mainAccPositions[ethMainPosIndex]['quantity'])
                            
                        else:
                            mainEthPosition = 0
                      else:
                            mainEthPosition = 0
            
                      masterPosition = mainEthPosition
                    
                      if len(positions) > 0:
                        
                        ethPosIndex = next((index for (index, d) in enumerate(positions) if d["marketCode"] == "ETH-USD-SWAP-LIN"), None)        
                        flexPosIndex = next((index for (index, d) in enumerate(positions) if d["marketCode"] == "FLEX-USD-SWAP-LIN"), None)
                        
                        if ethPosIndex is not None:
                            ethPosition = float(positions[ethPosIndex]['position'])
                        else:
                            ethPosition = 0
                    
                        masterPosition += ethPosition
                        
                      if masterPosition != 0:
                          
                          if masterPosition > 0:
                              directionHedge = 'SELL'
                          else:
                              directionHedge = 'BUY'
                          
                          usdPosition = masterPosition*price
                          if directionHedge == 'SELL':
                              drift = -1
                          else:
                              drift = 1
                          mid = price + drift*250                    
                          bid = str(mid-250)
                          offer = str(mid+250)
                          
                          # if timeoutMins == 120:
                          #     usdPosition = usdPosition*2
                          
                          if usdPosition < 0:
                              collateralQuantity=str(-usdPosition/flexPrice)
                          else:
                              collateralQuantity=str(usdPosition/flexPrice)
                              
                          hash_token = create_amm(directionHedge, asset, bid, offer, str(leverage), collateralAsset="FLEX", collateralQuantity=collateralQuantity)
                          time.sleep(10)
                          # get_amm_trades(hashToken)
                          
                          get_amm_orders(hash_token, bid, offer, drift)
                          
                
                          print('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))
                          logging.info('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))

                          
                          masterDf =  masterDf.append(pd.DataFrame({'hashToken':hash_token, 'status':'EXECUTING','direction':directionHedge,'startingPrice':price, 'minPrice':bid, 'maxPrice':offer,'startingMasterPosition':masterPosition, 'drift':drift, 'quantityTokens':(float(collateralQuantity)*flexPrice)/price, 'leverage':str(leverage), 'collateralAsset':"FLEX", 'collateralUsdValue':float(collateralQuantity)*flexPrice, 'timestampEntry':datetime.datetime.utcnow()}, index= [masterCount]))
            
                          print(masterDf.tail().astype(str))
                          logging.info(masterDf.tail().astype(str))
            
                          masterCount += 1
                          
                          print('Sleeping whilst Directional AMM hedges')
                          
                          time.sleep(timeoutMins*60)
                          
                          print('Redeeming {}'.format(hash_token))
                          logging.info('Redeeming {}'.format(hash_token))
                          masterDf.loc[masterDf['hashToken'] == hash_token, 'status'] = 'ENDED'
                    
                          try:
            
                         
                              redeem_amm(hash_token)
                              # get_amm_trades(hash_token)
                              
                          except Exception as e:
                              logging.exception("Exception occurred")
                              print(e) 
                          
                          time.sleep(10)

            if get_amm_info(hashToken)[0]['status'] == 'EXECUTING':
                
                      if [now.minute, now.second] in timeouts:
                          
                        print('Redeeming {}'.format(hashToken))
                        logging.info('Redeeming {}'.format(hashToken))
                        masterDf.loc[masterDf['hashToken'] == hashToken, 'status'] = 'ENDED'
                      
                        try:
                    
         
                            redeem_amm(hashToken)
                            # get_amm_trades(hashToken)
                            
                            
                        except Exception as e:
                            logging.exception("Exception occurred")
                            print(e)     
                            
                        
                      elif [datetime.datetime.utcnow().hour, datetime.datetime.utcnow().minute] not in timesList and [datetime.datetime.utcnow().minute, datetime.datetime.utcnow().second] not in timeouts and time.time() > float(get_amm_info(hashToken)[0]['createdAt'])/1000+300:  
                        
                        print('Redeeming {}'.format(hashToken))
                        logging.info('Redeeming {}'.format(hashToken))
                        masterDf.loc[masterDf['hashToken'] == hashToken, 'status'] = 'ENDED'
                        
                        try:
                    
                            redeem_amm(hashToken)
                            
                        except Exception as e:
                            logging.exception("Exception occurred")
                            print(e)     
        
                        positions = get_amm_info(hashToken)[0]['positions']
                        
                        assetData = get_price(asset)[0]
                
                        price = float(assetData['markPrice'])
        
                        mainAccPositions = get_positions()
                        
                        if mainAccPositions is not None:
                        
                            ethMainPosIndex = next((index for (index, d) in enumerate(mainAccPositions) if d["instrumentId"] == "ETH-USD-SWAP-LIN"), None)  
                            
                            if ethMainPosIndex is not None:
                            
                                mainEthPosition = float(mainAccPositions[ethMainPosIndex]['quantity'])
                                
                            else:
                                mainEthPosition = 0
                        else:
                                mainEthPosition = 0
                
                        masterPosition = mainEthPosition
                        
                        if len(positions) > 0:
                            
                            ethPosIndex = next((index for (index, d) in enumerate(positions) if d["marketCode"] == "ETH-USD-SWAP-LIN"), None)        
                            flexPosIndex = next((index for (index, d) in enumerate(positions) if d["marketCode"] == "FLEX-USD-SWAP-LIN"), None)
                            
                            if ethPosIndex is not None:
                                ethPosition = float(positions[ethPosIndex]['position'])
                            else:
                                ethPosition = 0
                        
                            masterPosition += ethPosition
                        
                            usdPosition = masterPosition*float(positions[ethPosIndex]['markPrice'])
                            
                            drift = -usdPosition/(quantityUsd*5)
                        
                            mid = price + drift*spread
                            
                            bid = str(mid-spread)
                            offer = str(mid+spread)
                        
                        elif masterPosition != 0:
                            
                            usdPosition = masterPosition*price
                            drift = -usdPosition/(quantityUsd*5)                
                            mid = price + drift*spread                    
                            bid = str(mid-spread)
                            offer = str(mid+spread)
                            
                        else:
                            mid = price                    
                            bid = str(mid-spread)
                            offer = str(mid+spread)
                            drift=0
                            
                        
                        hash_token = create_amm(direction, asset, bid, offer, str(leverage), collateralAsset="FLEX", collateralQuantity=str(quantity))
                        time.sleep(10)
                        # get_amm_trades(hashToken)
                        get_amm_orders(hash_token, bid, offer, drift)
                        
                        
                        print('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))
                        logging.info('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))

                        masterDf =  masterDf.append(pd.DataFrame({'hashToken':hash_token, 'status':'EXECUTING','direction':direction,'startingPrice':price, 'minPrice':bid, 'maxPrice':offer,'startingMasterPosition':masterPosition, 'drift':drift, 'quantityTokens':quantityUsd/price, 'leverage':str(leverage), 'collateralAsset':"FLEX", 'collateralUsdValue':str(quantity*flexPrice), 'timestampEntry':datetime.datetime.utcnow()}, index= [masterCount]))
                    
                        print(masterDf.tail().astype(str))
                        logging.info(masterDf.tail().astype(str))
                    
                        masterCount += 1
            
            working = get_amms()            
            if [datetime.datetime.utcnow().hour, datetime.datetime.utcnow().minute] not in timesList and [datetime.datetime.utcnow().minute, datetime.datetime.utcnow().second] not in timeouts and working == None:
                        
                        mainAccPositions = get_positions()
                        
                        if mainAccPositions is not None:
                        
                            ethMainPosIndex = next((index for (index, d) in enumerate(mainAccPositions) if d["instrumentId"] == "ETH-USD-SWAP-LIN"), None)  
                            
                            if ethMainPosIndex is not None:
                            
                                mainEthPosition = float(mainAccPositions[ethMainPosIndex]['quantity'])
                                
                            else:
                                mainEthPosition = 0
                        else:
                                mainEthPosition = 0
                
                        masterPosition = mainEthPosition
                        assetData = get_price(asset)[0]
                      
                        price = float(assetData['markPrice'])            
        
                        if masterPosition != 0:
                            
                            usdPosition = masterPosition*price
                            drift = -usdPosition/(quantityUsd*5)                
                            mid = price + drift*spread                    
                            bid = str(mid-spread)
                            offer = str(mid+spread)
                            
                        else:
                            mid = price                    
                            bid = str(mid-spread)
                            offer = str(mid+spread)
                            drift=0
                        
                        hash_token = create_amm(direction, asset, bid, offer, str(leverage), collateralAsset="FLEX", collateralQuantity=str(quantity))
                        
                        get_amm_orders(hash_token, bid, offer, drift)
                        time.sleep(10)
                        
                        print('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))
                        logging.info('Created new AMM with drift of {}, minPrice {}, maxPrice {},  {}. ------- Spot = {}, quantityUsd = {}, masterPosition = {}'.format(drift, bid, offer, hash_token, price, quantityUsd, masterPosition))

                        masterDf =  masterDf.append(pd.DataFrame({'hashToken':hash_token, 'status':'EXECUTING','direction':direction,'startingPrice':price, 'minPrice':bid, 'maxPrice':offer,'startingMasterPosition':masterPosition, 'drift':drift, 'quantityTokens':quantityUsd/price, 'leverage':str(leverage), 'collateralAsset':"FLEX", 'collateralUsdValue':str(quantity*flexPrice), 'timestampEntry':datetime.datetime.utcnow()}, index= [masterCount]))
                    
                        print(masterDf.tail().astype(str))
                        logging.info(masterDf.tail().astype(str))
                    
                        masterCount += 1
                
            assetData = get_price(asset)[0]
                      
            price = float(assetData['markPrice']) 
                        
            if masterPosition > ((quantityUsd*5)/price) or masterPosition < ((-quantityUsd*5)/price):
                    
                            print('Starting position hedging')
                            print('Current position is {}, greater than 5X max position'.format(masterPosition))
                            
                            logging.info('Starting position hedging')
                            logging.info('Current position is {}, greater than 5X max position'.format(masterPosition))
                        
                            if masterPosition > 0:
                                
                                side = "SELL"                    
                            else:
                                side = "BUY"
                            
                            clientOrderId = str(int(datetime.datetime.utcnow().timestamp()))+hashToken[-6:]
                            
                            orderType = "MARKET"
                            
                            if masterPosition < 0:        
                                size = masterPosition * -1          
                            else:
                                size = masterPosition
                    
                            resp = post_market_order(clientOrderId,asset, side, size, orderType)
                    
                            print(resp)
                            logging.info(resp)
                    
        if count % 20 == 0:  
                          manual = get_amm_info(hashToken)[0]
                          if manual['status'] == 'EXECUTING':
                              positions = manual['positions']

                              if len(positions) > 0:
                                
                                ethPosIndex = next((index for (index, d) in enumerate(positions) if d["marketCode"] == "ETH-USD-SWAP-LIN"), None)        
                                flexPosIndex = next((index for (index, d) in enumerate(positions) if d["marketCode"] == "FLEX-USD-SWAP-LIN"), None)
                                
                                if ethPosIndex is not None:
                                    ethPosition = float(positions[ethPosIndex]['position'])
                                else:
                                    ethPosition = 0
                              else: 
                                 ethPosition = 0
                          else: 
                            ethPosition = 0
                          print('Net position is {}, program profit is ${:,}'.format(masterPosition+ethPosition, calc_program_profit(startingFlex, startingUsd)))
                          logging.info('Net position is {}, program profit is ${:,}'.format(masterPosition+ethPosition, calc_program_profit(startingFlex, startingUsd)))

        count+=1
        masterDf.to_csv('/home/ec2-user/csvFiles/masterDf.csv')


  