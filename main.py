#%%
"""
TODO:
    if I want to put all bets in one table I get operational error
    line 77 try/except
"""
import requests_xml
import json

import pandas as pd
import collections

import sqlite3

import pickle


conn = sqlite3.connect('test__9900.db')

session = requests_xml.XMLSession()

r = session.get('http://xml.cdn.betclic.com/odds_en.xml')
xml = requests_xml.XML(xml=r.text)
res = json.loads(xml.json())

def parseSport(sport, mapping):
    nameOfSport = sport['@name']
    idOfSport = sport['@id']
    sportMapping = {'nameOfSport': nameOfSport, 'idOfSport': idOfSport}
    mapping['sport'].append(sportMapping)
    
    return (idOfSport, mapping)
    

def parseLeague(league, mapping):
    
    nameOfLeague = league['@name']
    idOfLeague = league['@id']
    leagueMapping = {'nameOfLeague': nameOfLeague, 'idOfLeague': idOfLeague}
    mapping['league'].append(leagueMapping)
    
    return (idOfLeague, mapping)
    

def parseMatch(match, mapping):
    nameOfMatch = match['@name']
    idOfMatch = match['@id']
    startOfMatch = match['@start_date']
    isStreaming = match['@streaming']
    matchMapping = {'nameOfMatch': nameOfMatch, 'idOfMatch': idOfMatch, 'startOfMatch': startOfMatch, 'isStreaming': isStreaming}
    mapping['match'].append(matchMapping)
    
    return (idOfMatch, mapping)


def parseAndHandleBet(bet, conn, mapping, idOfMatch, idOfLeague, idOfSport):
    betCode = bet['@code']
    betId = bet['@id']
    betName = bet['@name']
    betMapping = {'betCode': betCode, 'betName': betName, 'betId': betId}
    mapping['bet'].append(betMapping)
    
    #send data to sql
    dfTemp = pd.DataFrame(bet['choice'])
    dfTemp.to_sql('data', conn, index=False, if_exists='append')
    renameMapping = {col:col.replace('@', '').upper() for col in dfTemp.columns}#repair weird column names with @ in them
    dfTemp.rename(renameMapping, axis=1, inplace=True)
    dfTemp.to_sql('data', conn, index=False, if_exists='append')
    
    #connect all IDs
    IDs = {'betId': betId, 'idOfMatch': idOfMatch, 'idOfLeague': idOfLeague, 'idOfSport': idOfSport}
    mapping['ID'].append(IDs)
    
    return mapping

def parseAndHandleListOfBets(match, mapping, conn, parseMatch, idOfLeague, idOfSport, l):
    idOfMatch, mapping = parseMatch(match, mapping)
    try:
        ll = match['bets']['bet'][0]
    except:
        ll = match['bets']['bet']

    if type(match['bets']) == dict:
        mapping = parseAndHandleBet(ll, conn, mapping, idOfMatch, idOfLeague, idOfSport)
    else:
        for bet in match['bets']['bet']:
            mapping = parseAndHandleBet(bet[0], conn, mapping, idOfMatch, idOfLeague, idOfSport)
            
    return mapping

def leagueAndBets(league, mapping, idOfSport):
    idOfLeague, mapping = parseLeague(league, mapping)
    if type(league['match']) == dict:
        match = league['match']
        mapping = parseAndHandleListOfBets(match, mapping, conn, parseMatch, idOfLeague, idOfSport, False)
    elif type(league['match']) == list:
        for match in league['match']:
            mapping = parseAndHandleListOfBets(match, mapping, conn, parseMatch, idOfLeague, idOfSport, True)
    else:
        print('else!!')
        
    return mapping
            
def init():
    mapping = collections.defaultdict(list)
    for sport in res['sports']['sport']:
        idOfSport, mapping = parseSport(sport, mapping)
        if type(sport['event']) == dict:
            league = sport['event']
            mapping = leagueAndBets(league, mapping, idOfSport)
        else:
            for league in sport['event']:
                mapping = leagueAndBets(league, mapping, idOfSport)
    #now let send to sql all mapping tables
    for key in mapping:
        pd.DataFrame(mapping[key]).to_sql(key, conn, index=False, if_exists='append')

init()