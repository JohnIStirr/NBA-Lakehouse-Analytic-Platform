# Data Model

## Grain

### dim_team

One row per NBA team.

Primary Key

- team_id

---

### dim_player

One row per player.

Primary Key

- player_id

---

### fact_game

One row per team per game.

Primary Key

(game_id, team_id)
