from collections import Counter
from typing import Optional

import polars as pl
from polars import Boolean, Float64, Int64, Schema, String

from const import WORDS_TO_IGNORE

data_schema = Schema(
    [
        ("", Int64),
        ("app_id", Int64),
        ("app_name", String),
        ("review_id", Int64),
        ("language", String),
        ("review", String),
        ("timestamp_created", Int64),
        ("timestamp_updated", Int64),
        ("recommended", Boolean),
        ("votes_helpful", Int64),
        ("votes_funny", Int64),
        ("weighted_vote_score", Float64),
        ("comment_count", Int64),
        ("steam_purchase", Boolean),
        ("received_for_free", Boolean),
        ("written_during_early_access", Boolean),
        ("author.steamid", Int64),
        ("author.num_games_owned", Int64),
        ("author.num_reviews", Int64),
        ("author.playtime_forever", Float64),
        ("author.playtime_last_two_weeks", Float64),
        ("author.playtime_at_review", Float64),
        ("author.last_played", Float64),
    ]
)


def process_once_polars(raw_file_path: str) -> pl.DataFrame:
    raw_lf = pl.scan_csv(raw_file_path, schema=data_schema)

    important_fields_lf = (
        raw_lf.select(["app_name", "language", "review", "recommended", "author.playtime_forever"])
        .rename(
            {
                "app_name": "game_name",
                "recommended": "is_recommended",
                "author.playtime_forever": "reviewer_all_time_playtime",
            }
        )
        .filter(pl.col("language").str.to_lowercase() == "english")
    )

    processed_lf = (
        important_fields_lf.with_columns(
            review=pl.col("review").str.to_lowercase().str.split(" ").list.filter(~pl.element().is_in(WORDS_TO_IGNORE))
        )
        .group_by(["game_name"])
        .agg(most_common_word=pl.col("review").flatten().mode().first())
        .select([pl.col("game_name"), pl.col("most_common_word")])
    )

    sorted_lf = processed_lf.sort(by=["game_name"], descending=[False])
    return sorted_lf.collect()


def process_once_polars_df(raw_file_path: str) -> pl.DataFrame:
    raw_lf = pl.scan_csv(raw_file_path, schema=data_schema)

    important_fields_lf = (
        raw_lf.select(["app_name", "language", "review", "recommended", "author.playtime_forever"])
        .rename(
            {
                "app_name": "game_name",
                "recommended": "is_recommended",
                "author.playtime_forever": "reviewer_all_time_playtime",
            }
        )
        .filter(pl.col("language").str.to_lowercase() == "english")
    )

    def process_review_str(review: str) -> list[str]:
        split_word = review.lower().split(" ")
        filtered_words = [word for word in split_word if word not in WORDS_TO_IGNORE]
        return filtered_words

    def aggregate_reviews(all_review_words: Optional[list[list[str]]]) -> Optional[str]:
        if all_review_words is None:
            return None

        valid_reviews = (review_words for review_words in all_review_words if review_words is not None)
        flat_words = (word for valid_review in valid_reviews for word in valid_review if word is not None)

        counter = Counter(flat_words)
        most_common_words = counter.most_common(1)
        return most_common_words[0][0] if len(most_common_words) > 0 else None

    processed_lf = (
        important_fields_lf.with_columns(
            review=pl.col("review").map_elements(process_review_str, return_dtype=pl.List(String))
        )
        .group_by(["game_name"])
        .agg(most_common_word=pl.col("review").implode().map_elements(aggregate_reviews, return_dtype=pl.String))
        .select([pl.col("game_name"), pl.col("most_common_word")])
    )

    sorted_lf = processed_lf.sort(by=["game_name"], descending=[False])
    return sorted_lf.collect()
