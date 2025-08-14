from collections import Counter
import pandas as pd
from const import WORDS_TO_IGNORE


def _get_most_common_word(agg_words: pd.Series) -> str:
    counter = Counter([word for words in agg_words for word in words])
    return counter.most_common(1)[0][0]


def process_once_pandas(raw_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(
        raw_file_path,
        dtype={
            "": int,
            "app_id": int,
            "app_name": pd.StringDtype(storage="pyarrow"),
            "review_id": int,
            "language": pd.StringDtype(storage="pyarrow"),
            "review": pd.StringDtype(storage="pyarrow"),
            "timestamp_created": int,
            "timestamp_updated": int,
            "recommended": bool,
            "votes_helpful": int,
            "votes_funny": int,
            "weighted_vote_score": float,
            "comment_count": int,
            "steam_purchase": bool,
            "received_for_free": bool,
            "written_during_early_access": bool,
            "author.steamid": int,
            "author.num_games_owned": int,
            "author.num_reviews": int,
            "author.playtime_forever": float,
            "author.playtime_last_two_weeks": float,
            "author.playtime_at_review": float,
            "author.last_played": float,
        },
    )

    df = df[["app_name", "language", "review", "recommended", "author.playtime_forever"]].rename(
        columns={
            "app_name": "game_name",
            "recommended": "is_recommended",
            "author.playtime_forever": "reviewer_all_time_playtime",
        }
    )
    df = df[df["language"].str.lower() == "english"]

    df["review"] = df["review"].str.lower().str.split(" ")
    df["review"] = df["review"].map(
        lambda strings: [word for word in strings if word not in WORDS_TO_IGNORE]
        if (isinstance(strings, list))
        else [],
    )
    df = (
        df.groupby("game_name")
        .agg({"is_recommended": "mean", "reviewer_all_time_playtime": "mean", "review": _get_most_common_word})
        .rename(
            columns={
                "is_recommended": "percent_recommend",
                "reviewer_all_time_playtime": "avg_reviewer_playtime",
                "review": "most_common_word",
            }
        )
    )
    df["percent_recommend"] = df["percent_recommend"] * 100.0

    df = df.sort_values(by=["percent_recommend", "game_name"], ascending=[False, True])

    return df
