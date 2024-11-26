import requests
import pymysql, time, sys
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict, deque
from update import updater
import traceback

keys = ['battle_at', 'battle_id', 'battle_type', 'game_version', 'p1_area_id', 'p1_chara_id', \
'p1_lang', 'p1_name', 'p1_polaris_id', 'p1_power', 'p1_rank', 'p1_rating_before', 'p1_rating_change', \
    'p1_region_id', 'p1_rounds', 'p1_user_id', 'p2_area_id', 'p2_chara_id', 'p2_lang', 'p2_name', 'p2_polaris_id', \
        'p2_power', 'p2_rank', 'p2_rating_before', 'p2_rating_change', 'p2_region_id', \
            'p2_rounds', 'p2_user_id', 'stage_id', 'winner']

class Null:
    def __repr__(self) -> str:
        return "null"
    
    def __eq__(self, other):
        if type(other) == Null: return True
        else: return False

def defDict2Dict(obj: defaultdict):
    ret = {}
    for k in obj.keys():
        if type(obj[k]) == defaultdict:
            ret[k] = defDict2Dict(obj[k])
        else: ret[k] = obj[k]
    return ret

def matchParser(line, character_vs, character_statistics, player_character, player, area):
    player_id = line['p1_polaris_id']
    prefix_me = 'p1_'
    prefix_opp = 'p2_'

    battle_at = line['battle_at']
    version = line['game_version']
    didDraw = line[f'winner'] == 3
    
    for prefix in (prefix_me, prefix_opp):
        p_id = line[f'{prefix}polaris_id']
        isP1 = p_id == player_id
        p_area = line[f'{prefix}area_id']
        p_name = line[f'{prefix}name']

        if player[p_id][3] < battle_at:
            if player[p_id][1] == -1 or player[p_id][1]!= p_area:
                area[player[p_id][1]] -=1
                area[p_area] += 1
                player[p_id][1] = p_area
            player[p_id][2] = p_name
            player[p_id][3] = battle_at
        if player[p_id][4] > battle_at:
            player[p_id][4] = battle_at
        
        chara_id, version, didWin, rank = line[f'{prefix}chara_id'], line['game_version'], line['winner'] == 1 if isP1 else line['winner'] == 2, line[f'{prefix}rank']
        player_character[p_id][chara_id][0] += 1
        player_character[p_id][chara_id][1] += 1 if didWin else 0
        player_character[p_id][chara_id][4] += 1 if didDraw else 0
        if player_character[p_id][chara_id][3] < battle_at:
            player_character[p_id][chara_id][2] = rank
            player_character[p_id][chara_id][3] = battle_at
        
    chara_id = line[f'{prefix_me}chara_id']
    opp_chara_id = line[f'{prefix_opp}chara_id']
    didWin = line['winner'] == 1
    
    chara_ids = [min(chara_id, opp_chara_id), max(chara_id, opp_chara_id)]
    left_win = not(didWin) if opp_chara_id < chara_id else didWin
    character_vs[chara_ids[0]][chara_ids[1]][version][0] += 1
    character_vs[chara_ids[0]][chara_ids[1]][version][1] += 1 if left_win else 0
    character_vs[chara_ids[0]][chara_ids[1]][version][2] += 1 if didDraw else 0
    
    character_statistics[chara_id][version][0] += 1
    character_statistics[chara_id][version][1] += 1 if didWin else 0
    character_statistics[chara_id][version][2] += 1 if didDraw else 0
    
    character_statistics[opp_chara_id][version][0] += 1
    character_statistics[opp_chara_id][version][1] += 1 if not(didWin) else 0
    character_statistics[opp_chara_id][version][2] += 1 if didDraw else 0
    
def dataPutter(db, q: deque):
    character_vs = defaultdict(lambda : defaultdict(lambda : defaultdict(lambda : [0, 0, 0])))
    character_statistics = defaultdict(lambda : defaultdict(lambda : [0, 0, 0]))
    player_character = defaultdict(lambda : defaultdict(lambda : [0, 0, 0, 0, 0]))
    player = defaultdict(lambda : ["", -1, "", -1, 2**64])
    area = defaultdict(int)
    

    keys_str = str(tuple(keys))        
    keys_str = keys_str.replace("""'""", '`')
        
    while True:
        ret = q.popleft()
        if not len(ret): continue
        if type(ret) != pd.DataFrame: break

        ret = ret[keys]
        # area_id_na -> 99 치환
        ret['p1_area_id'] = ret['p1_area_id'].where(ret['p1_area_id'].notna(), 99)
        ret['p2_area_id'] = ret['p2_area_id'].where(ret['p2_area_id'].notna(), 99)
        
        # na -> null 치환
        ret = ret.where(~(ret.isna()), Null())
        
        # 이름 특문 치환
        ret['p1_name'] = ret['p1_name'].str.replace("\\", "\\\\")
        ret['p1_name'] = ret['p1_name'].str.replace("'", "\\'")
        ret['p2_name'] = ret['p2_name'].str.replace("\\", "\\\\")
        ret['p2_name'] = ret['p2_name'].str.replace("'", "\\'")
        
        ret.apply(lambda x : matchParser(x, character_vs, character_statistics, player_character, player, area), axis=1)
        values_str = ", ".join(list(map(lambda x: str(tuple(x)), ret.values.tolist())))               
                    
        sql = f'''INSERT INTO match_history {keys_str} VALUE {values_str}'''
        db.cursor().execute(sql)

    return character_vs, character_statistics, player_character, player, area
        
def dataGetter(q: deque, getter_idx_q: deque):
    while True:
        latest_match_time, ender = getter_idx_q.popleft()
        if latest_match_time <= ender: break

        ret = requests.get(f'https://wank.wavu.wiki/api/replays?before={latest_match_time}').json()

        if ret: ret = pd.DataFrame(ret)[keys]
        else: ret = pd.DataFrame(columns=keys)
        ret = ret.where(ret['game_version'] == int(dbname)).dropna()
        ret = ret.sort_values(by=['battle_at'], ascending=False)
        ret = ret.reset_index(drop=True)

        if ret.shape[0]: 
            if ret['battle_at'].iloc[0] == ret['battle_at'].iloc[-1]:
                q.append(ret)
                latest_match_time = ret['battle_at'].iloc[-1] - 1
            else:
                latest_match_time = ret['battle_at'].iloc[-1]
                ret = ret.where((ret['battle_at'] > ret['battle_at'].iloc[-1]) & (ret['battle_at'] > ender)).dropna()
                q.append(ret)
                
        else: latest_match_time -= 700
        
        getter_idx_q.append((latest_match_time, ender))
    
if __name__ == '__main__':
    dbname = sys.argv[1]
    prev_time = datetime(2024, 1, 1)
    one_minute = timedelta(minutes=1)
    isLive = True
    
    while True:
        now = datetime.now()
        delta = now - prev_time
        if now - prev_time < one_minute:
            time.sleep((one_minute - delta).total_seconds())
        prev_time = datetime.now()

        try:
            ret = requests.get(f'https://wank.wavu.wiki/api/replays').json()
            if ret: start = ret[0].get('battle_at')
            else: start = int(prev_time.timestamp())
            db = pymysql.Connect(host='localhost', port=3306, user='root', passwd='ehd**!!wns29', db=dbname)
            with db.cursor() as cur:
                sql = "SELECT MAX(battle_at) FROM match_history"
                cur.execute(sql)
                end = int(cur.fetchall()[0][0])
        
            getter_idx = [start, end]
            getter_idx_q = deque()
            
            for i in range(1):
                getter_idx_q.append((getter_idx[i], getter_idx[i + 1]))
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), f'--- Task Start {(start, end)}')
            
            q = deque()
            v = 0
            
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Api Pulling Start')
            dataGetter(q, getter_idx_q)
            q.append(["End"])
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Api Pulling End')
            
            
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Match History Pushing Start')
            character_vs, character_statistics, player_character, player, area = dataPutter(db, q)
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Match History Pushing End')
            
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Other Data Pushing Start')
            updater(db, character_vs, character_statistics, player_character, player, area)
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Other Data Pushing End')
            
            
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Commit Start')
            db.commit()
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Commit End')
            db.close()
        except Exception as e:
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Error Log')
            print(traceback.format_exc())
            print(datetime.now().strftime("%Y-%m-%d, %H%M%S"), '--- Error Log End')