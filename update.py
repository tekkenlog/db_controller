from tqdm import tqdm
import sys
from datetime import datetime
import pymysql

def updater(db, character_vs, character_statistics, player_character, player, area):
    with db.cursor() as cur:
        sql_head = f"""insert into player (`id`, `area_id`, `player_name`, `last_played`, `first_played`) value """
        sql_tail = []
        for player_id in player.keys():
            sql = f"""SELECT * FROM player where id = '{player_id}'"""
            cnt = cur.execute(sql)
            if not cnt:
                area_id = player[player_id][1]
                player_name = player[player_id][2]
                recent_match_at = player[player_id][3]
                latest_match_at = player[player_id][4]
                area[area_id] += 1
                sql_tail.append(f"""('{player_id}', {area_id}, '{player_name}', {recent_match_at}, {latest_match_at})""")
            else:
                ret = cur.fetchall()[0]
                last_played = ret[3]
                
                new_player_name = player[player_id][2]
                new_last_played = player[player_id][3]
                
                if last_played < new_last_played:
                    prev_area = ret[1]
                    new_area = player[player_id][1]
                    area[prev_area] -= 1
                    area[new_area] += 1
                    sql = f"""UPDATE player SET area_id = {new_area}, `player_name` = '{new_player_name}', `last_played` = {new_last_played} where id = '{player_id}'"""
                    cur.execute(sql)
                    
            if len(sql_tail) >= 5000:
                cur.execute(sql_head + ", ".join(sql_tail))
                sql_tail = []
        if sql_tail:
            cur.execute(sql_head + ", ".join(sql_tail))
            
            
    # player_character Table Update
    with db.cursor() as cur:
        counter = 0
        sql_head = f"""INSERT INTO player_character (`player_id`, `character_id`, `match_count`, `win_count`, `rank`, `lastplayed`, `draw_count`) value """
        sql_tail = []
        for player_id in player_character.keys():
            for t_chara_id in player_character[player_id].keys():
                sql = f"""SELECT * FROM player_character where player_id = '{player_id}' and character_id = {t_chara_id}"""
                cnt = cur.execute(sql)
                if not cnt:
                    sql_tail.append(f"""('{player_id}', {t_chara_id}, {player_character[player_id][t_chara_id][0]}, {player_character[player_id][t_chara_id][1]}, {player_character[player_id][t_chara_id][2]}, {player_character[player_id][t_chara_id][3]}, {player_character[player_id][t_chara_id][4]})""")
                else:
                    ret = cur.fetchall()[0]
                    last_played = ret[5]
                    
                    adder_match_count = player_character[player_id][t_chara_id][0]
                    adder_win_count = player_character[player_id][t_chara_id][1]
                    new_rank = player_character[player_id][t_chara_id][2]
                    new_last_played = player_character[player_id][t_chara_id][3]
                    adder_draw_count = player_character[player_id][t_chara_id][4]
                    
                    sql = f"""UPDATE player_character SET match_count = match_count + {adder_match_count}, \
                        win_count = win_count + {adder_match_count}, draw_count = draw_count + {adder_draw_count}"""
                    if last_played < new_last_played:
                        sql += f""", lastplayed = {new_last_played}, `rank` = {new_rank}"""
                    sql += f""" where player_id = '{player_id}' and character_id = {t_chara_id}"""
                    cur.execute(sql)
                    
                if len(sql_tail) >= 5000:
                    cur.execute(sql_head + ", ".join(sql_tail))
                    sql_tail = []
        if sql_tail:
            cur.execute(sql_head + ", ".join(sql_tail))
                    
    # character_vs Table update
    with db.cursor() as cur:
        sql_head = f"""INSERT INTO character_vs (`character_id_1`, `character_id_2`, `version`, `total_match`, `left_wins`, `draw_count`) value """
        sql_tail = []
        for t_chara_id_1 in character_vs.keys():
            for t_chara_id_2 in character_vs[t_chara_id_1].keys():
                for version in character_vs[t_chara_id_1][t_chara_id_2].keys():
                    sql = f"""SELECT * FROM character_vs where character_id_1 = {t_chara_id_1} and character_id_2 = {t_chara_id_2} and version = {version}"""
                    cnt = cur.execute(sql)
                    if not cnt:
                        sql_tail.append(f"({t_chara_id_1}, {t_chara_id_2}, {version}, {character_vs[t_chara_id_1][t_chara_id_2][version][0]}, {character_vs[t_chara_id_1][t_chara_id_2][version][1]}, {character_vs[t_chara_id_1][t_chara_id_2][version][2]})")
                    else:
                        ret = cur.fetchall()[0]
                        adder_total_match = character_vs[t_chara_id_1][t_chara_id_2][version][0]
                        adder_left_wins = character_vs[t_chara_id_1][t_chara_id_2][version][1]
                        adder_draw_count = character_vs[t_chara_id_1][t_chara_id_2][version][2]
                        
                        sql = f"""UPDATE character_vs SET total_match = total_match + {adder_total_match}, left_wins = left_wins + {adder_left_wins}, \
                            draw_count = draw_count + {adder_draw_count} """
                        sql += f"""where character_id_1 = {t_chara_id_1} and character_id_2 = {t_chara_id_2} and version = {version}"""
                        cur.execute(sql)
        if sql_tail:
            cur.execute(sql_head + ", ".join(sql_tail))

    # character_statistics Table update
    with db.cursor() as cur:
        sql_head = "INSERT INTO character_statistics (`character_id`, `version`, `total_match`, `win_count`, `draw_count`) value "
        sql_tail = []
        for t_chara_id in character_statistics.keys():
            for version in character_statistics[t_chara_id].keys():
                sql = f"""SELECT * FROM character_statistics where character_id = {t_chara_id} and version = {version}"""
                cnt = cur.execute(sql)
                if not cnt:
                    sql_tail.append(f"""({t_chara_id}, {version}, {character_statistics[t_chara_id][version][0]}, {character_statistics[t_chara_id][version][1]}, {character_statistics[t_chara_id][version][2]})""")
                else:
                    ret = cur.fetchall()[0]
                    adder_total_match = character_statistics[t_chara_id][version][0]
                    adder_win_count = character_statistics[t_chara_id][version][1]
                    adder_draw_count = character_statistics[t_chara_id][version][2]
                    sql = f"""UPDATE character_statistics SET total_match = total_match + {adder_total_match}, \
                        win_count = win_count + {adder_win_count}, draw_count = draw_count + {adder_draw_count} """
                    sql += f"""where character_id = {t_chara_id} and version = {version}"""
                    cur.execute(sql)
        if sql_tail:       
            cur.execute(sql_head + ", ".join(sql_tail))
            
    with db.cursor() as cur:
        for area_id in area.keys():
            sql = f"""update area set user_count = user_count + {area[area_id]} where id = {int(area_id)}"""
            cur.execute(sql)