from data.entities.entity_base import EntityBase
import polars as pl

class DiaryEntries(EntityBase):
    @property
    def entity_name(self) -> str:
        return "diary_entries"

    @property
    def additional_schema(self) -> dict:
        return {
            "UserId": pl.Utf8,
            "Food": pl.Utf8,
            "ConsumedAt": pl.Datetime("us", "UTC"),
            "Notes": pl.Utf8
        }