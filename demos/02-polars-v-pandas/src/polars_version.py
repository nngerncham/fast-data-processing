import polars as pl
from polars import Boolean, Float64, Int64, Schema, String

from const import WORDS_TO_IGNORE


def process_once_polars(raw_file_path: str) -> pl.DataFrame:
    raw_lf = pl.scan_csv(
        raw_file_path,
        schema=Schema(
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
        ),
    )

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
        .agg(
            percent_recommend=pl.col("is_recommended").cast(pl.Int32).mean().mul(pl.lit(100.0)).round(3),
            most_common_word=pl.col("review").flatten().mode().first(),
            avg_reviewer_playtime=pl.col("reviewer_all_time_playtime").mean(),
        )
        .select(
            [
                pl.col("game_name"),
                pl.col("percent_recommend"),
                pl.col("most_common_word"),
                pl.col("avg_reviewer_playtime"),
            ]
        )
    )

    sorted_lf = processed_lf.sort(by=["percent_recommend", "game_name"], descending=[True, False])

    return sorted_lf.collect()


def process_once_polars_df(raw_file_path: str) -> pl.DataFrame:
    raw_lf = pl.read_csv(
        raw_file_path,
        schema=Schema(
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
        ),
    )

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
        .agg(
            percent_recommend=pl.col("is_recommended").cast(pl.Int32).mean().mul(pl.lit(100.0)).round(3),
            most_common_word=pl.col("review").flatten().mode().first(),
            avg_reviewer_playtime=pl.col("reviewer_all_time_playtime").mean(),
        )
        .select(
            [
                pl.col("game_name"),
                pl.col("percent_recommend"),
                pl.col("most_common_word"),
                pl.col("avg_reviewer_playtime"),
            ]
        )
    )

    sorted_lf = processed_lf.sort(by=["percent_recommend", "game_name"], descending=[True, False])

    return sorted_lf
