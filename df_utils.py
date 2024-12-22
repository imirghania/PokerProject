import pandas as pd
from itertools import chain
from typing import Literal
from enum import Enum
from tqdm import tqdm


def merge_dicts(dicts_list:list[dict]):
    combined_dict = {}
    for d in tqdm(dicts_list, desc="Merging dicts", total=len(dicts_list), unit="dict"):
        for key, value in d.items():
            combined_dict.setdefault(key, []).append(value)
    return combined_dict


def get_names_cols_df(df:pd.DataFrame):
    return df.filter(regex=r'player(\d+)_name')


def get_player_hands(df:pd.DataFrame, player_name:str):
    names_df = get_names_cols_df(df)
    player_hands_df = df[names_df.apply(lambda row: player_name in row.values, axis=1)]
    return player_hands_df


def get_unique_names(df:pd.DataFrame):
    all_names = []
    player_names_df = df.filter(regex=r"player\d+_name")
    name_columns = player_names_df.columns
    for col in name_columns:
        all_names.append(player_names_df[col].unique().tolist())
    unique_player_names = list(set(list(chain(all_names))[0]))
    return unique_player_names


def hand_winning_test(hand_data:pd.Series, player_name:str):
    players_count = hand_data["players_num"]
    player_names = hand_data.filter(regex=r'player(\d+)_name')
    player_position_index = player_names[:players_count].values.tolist().index(player_name)
    player_total_win_position = f"player{player_position_index+1}_total_win"
    players_total_wins = hand_data.filter(regex=r'player(\d+)_total_win').dropna()
    player_total_win = players_total_wins[player_total_win_position]
    
    return (player_total_win >= players_total_wins).all()


def extract_player_position(data:pd.Series, player_name:str):
    """All the rows of the DataFrame should include the player_name"""
    players_number = data["players_num"]
    player_names = data.filter(regex=r'player(\d+)_name')
    player_position_index = player_names[:players_number].values.tolist().index(player_name)
    return player_position_index


class Action(Enum):
    BLIND = 'B'
    FOLD = 'f'
    CHECK = 'k'
    BET = 'b'
    CALL = 'c'
    RAISE = 'r'
    ALL_IN = 'A'
    QUIT = 'Q'
    KICKED = 'K'


def count_player_action(data:pd.Series, player_name:str, action:Action):
    player_position = extract_player_position(data, player_name)
    player_bets = data.filter(regex=rf'player{player_position+1}_bet')
    player_bets_str = ''.join([s for s in player_bets.tolist() if s!='-'])
    return player_bets_str.count(action.value)


def count_player_actions_type(data:pd.Series, player_name:str, action_type:Literal["all", "aggressive", "passive"]="all"):
    player_position = extract_player_position(data, player_name)
    player_bets = data.filter(regex=rf'player{player_position+1}_bet')
    player_bets_str = ''.join([s for s in player_bets.tolist() if s!='-'])
    result = len(player_bets_str)
    aggressive_actions_count = sum([count_player_action(data, player_name, action) for action in (Action.BET, Action.RAISE, Action.ALL_IN)])
    passive_actions_count = sum([count_player_action(data, player_name, action) for action in (Action.FOLD, Action.CHECK)])
    
    if action_type == "aggressive":
        result = aggressive_actions_count     
    elif action_type == "passive":
        result = passive_actions_count     
        
    return result


def get_player_hand_bet(hand:pd.Series, player_name):
    player_position = extract_player_position(hand, player_name)
    return hand.filter(regex=rf"player{player_position+1}_total_bet").item()


def get_player_hand_bankroll(hand:pd.Series, player_name):
    player_position = extract_player_position(hand, player_name)
    return hand.filter(regex=rf"player{player_position+1}_bankroll").item()


def get_player_hands_total_bets(player_df, player_name):
    player_total_bets = player_df.apply(get_player_hand_bet, player_name=player_name, axis=1)
    return player_total_bets.sum().item()


def calculate_hand_profit(hand:pd.Series, player_name):
    player_position = extract_player_position(hand, player_name)
    is_winner = hand_winning_test(hand, player_name)
    profit = (
        (hand["pot_size_showdown"] - hand[f"player{player_position+1}_total_bet"])
        if is_winner 
        else -hand[f"player{player_position+1}_total_bet"]
    )
    return profit


def calculate_aggression_factor(player_hands_df:pd.DataFrame, player_name:str):
    player_total_bets = player_hands_df.apply(
                                    count_player_action, 
                                    player_name=player_name, 
                                    action=Action.BET, 
                                    axis=1).sum()
    player_total_raises = player_hands_df.apply(
                                    count_player_action, 
                                    player_name=player_name, 
                                    action=Action.RAISE, 
                                    axis=1).sum()
    player_total_all_ins = player_hands_df.apply(
                                    count_player_action, 
                                    player_name=player_name, 
                                    action=Action.ALL_IN, 
                                    axis=1).sum()
    player_total_calls = player_hands_df.apply(
                                    count_player_action, 
                                    player_name=player_name, 
                                    action=Action.CALL, 
                                    axis=1).sum()
    numerator = player_total_bets + player_total_raises + player_total_all_ins
    denumenator = player_total_calls if player_total_calls else 1
    return (numerator / denumenator).round(3).item()


def calculate_hand_aggression_factor(hand:pd.Series, player_name:str):
    player_total_bets = count_player_action(hand, player_name=player_name, action=Action.BET)
    player_total_raises = count_player_action(hand, player_name=player_name, action=Action.RAISE)
    player_total_all_ins = count_player_action(hand, player_name=player_name, action=Action.ALL_IN)
    player_total_calls = count_player_action(hand, player_name=player_name, action=Action.CALL)

    numerator = player_total_bets + player_total_raises + player_total_all_ins
    denumenator = player_total_calls if player_total_calls else 1
    return round(numerator / denumenator, 3)



