# v0101 MEXC POSITION TPSL BY POSITION

- Uses native MEXC `/api/v1/private/stoporder/place` by `positionId` for TP/SL.
- Reads live `positionId` and exact `holdVol` from `/position/open_positions`.
- Places TP and SL as one position-attached protection order instead of two standalone plan orders.
- Keeps hard verification and emergency close behavior from v0100.
