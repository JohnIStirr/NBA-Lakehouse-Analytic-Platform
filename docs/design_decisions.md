# Design Decisions

## Decision 1

Problem

Game summary contains multiple rows because a game may be broadcast on multiple TV networks.

Impact

Joining directly duplicated fact records.

Solution

Aggregated broadcaster names before joining.

Result

Maintained one row per team per game.
  
## Decision 2

Problem

Many games did not exist in game_summary.

Solution

Derived season from season_id instead of relying on game_summary.

Reason

The game table is the authoritative source for season information.

## Decision 3

Problem

Original game table stores home and away statistics in one record.

Solution

Normalized into two rows.

Benefits

- Simpler analytics
- Cleaner star schema
- Easier aggregations
