# Daily Log

The csv files in this directory is the daily log entries of train operations, extracted from SICP log and timetable. The updated version should be obtained from the remote VM.

## Filename

Filenames follow the convention of SICP log files, e.g. `25Dec19.csv` contains log on `2019/12/25`

## Columns

- `station`: e.g. `MOK`
  - `PRE` in SICP are denoted as `MOK` due to historical reasons. Correction has been made according to track numbers, which are manually extracted from the track diagram, when producing the csv files.
- `track`: track number, e.g. `40`
- `train`: logical train number, e.g. `GC01`
  - First two alphabets carry information about the operating route of trains. Details can be found in the corresponding timetable.
  - Last two numbers are the serial number of trains. Same numbers corresponds to the same physical train serving different routes under normal operations, e.g. `GC01` & `QC01`.
- `act_arr_time`: actual arrival time, recorded as the train entering the track in SICP log
- `act_dep_time`: actual departure time, recorded as the train leaving the track in SICP log
- `act_occupied_time`: actual occupied time of the track, derived as the difference between `act_dep_time` and `act_arr_time`
- `act_travelling_time`: actual travelling time of the train on this track, derived as the difference between `act_arr_time` on next track and on this track by the same train
  - `act_occupied_time` considers the train as an extended object and `act_travelling_time` considers the train as a point. Hence, `act_occupied_time` approximates dwell time while `act_travelling_time` approximates the journey time
- `stop`: whether this log entry is on platform track
  - Platform tracks are where trains stop during dwell time in a station
  - Platform tracks are manually extracted from the track diagram
- `arr_time`: scheduled arrival time in timetable
- `dep_time`: scheduled departure time in timetable
- `arr_delay`: arrival delay, defined as the difference between actual and scheduled arrival time
- `dep_delay`: departure delay, defined as the difference between actual and scheduled departure time
