import os
from datetime import datetime
import polars as pl


def format_dt(dt: datetime) -> str:
    """Return date and hr in filename format form

    Args:
        dt (datetime): datetime

    Returns:
        str: date & hour in hires filename format
    """
    date = str(dt.date()).replace("-", "_")

    hr = dt.hour * 100
    if hr == 0:
        hr = "0000"
    elif hr < 1000:
        hr = "0" + str(hr)
    else:
        hr = str(hr)

    return date, hr


def filter_directory(locid: str, sdt: datetime, edt: datetime):

    try:
        # return locid in filename format
        locid = ((5 - len(locid)) * "0") + locid

        # locate directory with files and create list
        path = os.getenv("DIRECTORY") + "Ctrl" + locid
        dir_list = os.listdir(path)

        # format datetime to filename date & hr format
        sdate, stime = format_dt(sdt)
        edate, etime = format_dt(edt)

        start_file_name = f"TRAF_{locid}_{sdate}_{stime}.csv"
        idx = dir_list.index(start_file_name)

        end_file_name = f"TRAF_{locid}_{edate}_{etime}.csv"
        idx_end = dir_list.index(end_file_name)

        # **Add hr if minute in end datetime
        if edt.minute > 0:
            idx_end += 1
        # print(idx, idx_end)

        dir_list = dir_list[idx:idx_end]

    # Typ error if file not found in directory, return empty list
    except Exception as err:
        print(err)
        return [], path
    return dir_list, path


def clean_csvs(dir_list: list, path: str):

    # ===========================
    #      Read Csv Data
    #   Clean/Format data
    #   Create one df from selected files
    # ===========================

    df_holder = []

    for file in dir_list:
        df = pl.read_csv(
            source=path + "/" + file,
            has_header=False,
            skip_rows=6,
            new_columns=["dt", "event_code", "parameter"],
        )

        df_holder.append(df)

    # concat data
    newdf = pl.concat(df_holder)

    # Format columns
    newdf = newdf.with_columns(
        pl.col("dt").str.to_datetime(r"%-m/%d/%Y %H:%M:%S%.3f"),
        pl.col("event_code").str.replace_all(" ", ""),
        pl.col("parameter").str.replace_all(" ", ""),
    ).with_columns(
        pl.col("event_code").str.to_integer(),
        pl.col("parameter").str.to_integer(),
    )

    return newdf.sort(by="dt")


def singles_wparams(df_ecodes: pl.DataFrame, df_data: pl.DataFrame) -> pl.DataFrame:
    """Modify event descriptor for event codes that change depending on parameters.
    # ex. Unit Flash - Preempt (173,8); Unit Flash - MMU (173,6)

    Args:
        df_ecodes (pl.DataFrame): event codes read from csv
        df_data (pl.DataFrame): dataset read from purdue csv file

    Returns:
        pl.DataFrame: modified event descriptors
    """

    # Temp column to match event/parameter to event/parameter in data
    df_ecodes = df_ecodes.with_columns(
        temp=pl.col("event_code").cast(pl.String)
        + pl.lit("-")
        + pl.col("event_param").cast(pl.String)
    )

    df_data = (
        df_data.with_columns(
            temp=pl.col("event_code").cast(pl.String)
            + pl.lit("-")
            + pl.col("parameter").cast(pl.String)
        )
        .with_columns(
            event_descriptor=pl.col("temp").replace_strict(
                old=df_ecodes["temp"],
                new=df_ecodes["event_description"],
                default=pl.col("event_descriptor"),
            )
        )
        .drop("temp")
    )

    return df_data


def flash_periods(df_data: pl.DataFrame, sdt: datetime, edt: datetime):
    """Determine nonFlash and Flash periods.
    During signal flash existing events are terminated
    Flash periods will help determine stop or starting points
    for events; especially orphan events terminated by flash

    Args:
        df_data (pl.DataFrame): df to process
        sdt (datetime): start datetime
        edt (datetime): end datetime

    Returns:
        _type_: nonFlashPeriods and flashPeriods in list of tuples format
    """

    # Search for flash events in df_data
    # 200-15 (start flash), 201-15 (end flash)
    # OR 173-6 (start flash), 173-2 (end flash)

    #! TODO: Test to see what flash codes work better; SEE GRANTS LAKE FLASH EVENT ON 12-23
    # other, automatic, local, cu, mmu, startup, preempt flash
    # flash_codes = ["173-1", "173-3", "173-4", "173-5", "173-6", "173-7", "173-8"]
    # exit_flash_code = "173-2"
    ec_map = {
        "173-1": 1,
        "173-2": 0,
        "173-3": 1,
        "173-4": 1,
        "173-5": 1,
        "173-6": 1,
        "173-7": 1,
        "173-8": 1,
    }

    # filter out flash events
    flash_events = df_data.with_columns(
        ec_param_code=(
            pl.col("event_code").cast(pl.Utf8) + "-" + pl.col("parameter").cast(pl.Utf8)
        ).replace_strict(ec_map, default=-1)
    ).filter(
        # pl.col("ec_param_code").is_in([*flash_codes, exit_flash_code]),
        pl.col("ec_param_code").is_in([0, 1]),
    )

    nonFlashPeriod = []
    flashPeriod = []

    # If flash events detected
    if flash_events.height > 0:

        # ============================================
        #   Add sdt & edt rows to detected flash intervals
        # ============================================

        # sdt_code = "173-2"
        sdt_code = 0
        fil_val = 1
        # if flash_events["ec_param_code"].first() == "173-2":
        if flash_events["ec_param_code"].first() == 0:
            # sdt_code = flash_codes[0]
            sdt_code = 1
            fil_val = 0

        # edt_code = "173-2"
        edt_code = 0
        # if flash_events["ec_param_code"].last() == "173-2":
        if flash_events["ec_param_code"].last() == 0:
            # edt_code = flash_codes[0]
            edt_code = 1

        start_end_dt = [
            {
                "dt": sdt,
                "event_code": 0,
                "parameter": None,
                "event_descriptor": None,
                "ec_param_code": sdt_code,
            },
            {
                "dt": edt,
                "event_code": 0,
                "parameter": None,
                "event_descriptor": None,
                "ec_param_code": edt_code,
            },
        ]

        # start and end events in df form
        se = pl.DataFrame(data=start_end_dt).with_columns(
            pl.col("dt").dt.cast_time_unit("ms")
        )

        # ? =================== Test Code ===================
        # fi = flash_events.vstack(se).sort("dt")
        # print(fi)
        # ? =================================================

        flash_events = (
            flash_events.vstack(se)
            .sort("dt")
            .filter(
                pl.col("ec_param_code")
                != pl.col("ec_param_code").shift(n=1, fill_value=fil_val)
            )
            .to_dicts()
        )

        i = 0
        while i < len(flash_events) - 1:
            if (
                # flash_events[i]["ec_param_code"] == exit_flash_code
                flash_events[i]["ec_param_code"] == 0
                # and flash_events[i + 1]["ec_param_code"] in flash_codes
                and flash_events[i + 1]["ec_param_code"] == 1
            ):
                nonFlashPeriod.append(
                    (flash_events[i]["dt"], flash_events[i + 1]["dt"])
                )
                i += 1
            else:
                flashPeriod.append((flash_events[i]["dt"], flash_events[i + 1]["dt"]))
                i += 1

    # If no mmu flash events detected
    nonFlashPeriod = [(sdt, edt)]

    # print("nonFlashPeriods: ", nonFlashPeriod)
    # print("FlashPeriods: ", flashPeriod)

    return nonFlashPeriod, flashPeriod


def event_intervals(
    ec_pairs: pl.DataFrame,
    df_data: pl.DataFrame,
    nonFlashPeriod: list[tuple[datetime, datetime]],
) -> pl.DataFrame:
    """Return start and end intervals for event codes specified in ec_pairs.
    These events with give phase, overlap, and operational status in final grid viz.
    ex. Phase Green (ec=1, ec=7). The parameter not shown determines the phase for example case

    Args:
        ec_pairs (pl.Dataframe): event code pair dataframe [(event_start_code, event_end_code, event_descriptor), ...]
        df_data (pl.DataFrame): hires data to process

    Returns:
        list[pl.DataFrame]: _description_
    """

    # ================================================
    #       Determine Intervals for all events
    # like phases, overlaps, etc during nonFlashPeriods
    # ? these events have "different start/event ecodes and same parameters"
    # ================================================

    nonFlash_holder = []

    for period in nonFlashPeriod:

        # current non-flash period to calculate ec_pair start and end
        current_period = df_data.filter(
            pl.col("dt").is_between(period[0], period[1]),
        )

        # iterates only pairs with parameters, if pair does not exit it is skipped
        for ec_pair in ec_pairs.to_dicts():

            # Return series of all unigue parameters codes for current event_start codes
            eventcode_params = current_period.filter(
                pl.col("event_code") == ec_pair["event_start"],
            )["parameter"].unique()

            for param in eventcode_params:

                # Test code
                # print(
                #     f"ec_start: {ec_pair['event_start']}, ec_end: {ec_pair['event_end']}: param: {param}"
                # )

                df_ec = current_period.filter(
                    pl.col("event_code").is_in(
                        [ec_pair["event_start"], *ec_pair["event_end"]]
                    ),
                    pl.col("parameter") == param,
                )

                # Test code to see pattern of multi pair scenerios
                # if ec_pair["event_start"] in [62]:
                # print(df_ec)
                # continue

                # df_ec.write_csv("dfec.csv")
                # current_period.write_csv("current.csv")

                # ================================================
                #      Orphan Event From Previous Period
                #
                # Add row for orphan event_end codes (ec_pair[1])
                # with start time per user requested start_dt
                # (ie. No matching event_start codes)
                # Previous Code discarded orphan events
                # ie: orphaned events that started prior to requested time
                # *The added assumed event start times are marked
                # *with '**' in the Event descriptor
                # TODO: removed as it was blurring results due to multiple end event code possibilities, revisit in future
                # ================================================
                # if df_ec["event_code"].first() in ec_pair["event_end"]:

                #     new_row = [period[0], ec_pair["event_start"], param, "**"]

                #     new = pl.DataFrame(
                #         data=[new_row],
                #         schema=["dt", "event_code", "parameter", "event_descriptor"],
                #         orient="row",
                #         # Have to cast dt to milliseconds
                #     ).with_columns(pl.col("dt").dt.cast_time_unit("ms"))

                #     # Concat old to new df
                #     df_ec = pl.concat(items=[new, df_ec], how="vertical")

                # ================================================
                #      Orphan Events at End of Period
                #
                # Add row for orphan event_start codes (ec_pair[0])
                # with start time per user requested start_dt
                # (ie No Matching event_end code)
                # ie: orphaned events that did not end before
                # requested end_dt
                # *The added assumed event end times are marked
                # *with '**' in the Event descriptor
                # ================================================
                if df_ec["event_code"].last() == ec_pair["event_start"]:

                    #! Add arbituary event_end code[0]; should not matter?
                    new_row = [period[1], ec_pair["event_end"][0], param, "**"]

                    new = pl.DataFrame(
                        data=[new_row],
                        schema=["dt", "event_code", "parameter", "event_descriptor"],
                        orient="row",
                        # Have to cast dt to milliseconds
                    ).with_columns(pl.col("dt").dt.cast_time_unit("ms"))

                    # Concat old to new df
                    df_ec = pl.concat(items=[df_ec, new], how="vertical")

                # ================================================
                #          Shift to Check On/off pattern
                #
                # Shift event codes to compare
                # filter and keeps event_codes that DO NOT MATCH (ie on/off pattern)
                # Removes events that do not follow on/off pattern
                # ie [1,2,1,2,2,1] removes on of the 2,2
                # Note: Used to remove orphan end events from previous
                # period but now handle these events w/added logic
                # ================================================

                # Only for pairs with 2 unique codes
                if len(ec_pair["event_end"]) == 1:

                    # TODO: fill value with event_end[1] is an assumption that can fail?...better long term solution?
                    # Becasue ec-pair-event_end is a list, we assume
                    #  second event_code to use as fill_vallue
                    fill_val = df_ec["event_code"].item(1)

                    df_ec = df_ec.filter(
                        pl.col("event_code")
                        != pl.col("event_code").shift(n=1, fill_value=fill_val)
                    )

                # For pairs that have more than one event end(more than two event codes)
                else:

                    ec_event = df_ec.to_dicts()
                    remove_rows = []

                    # drop repeating end codes
                    i = 0
                    while i < (df_ec.height - 1):
                        if (ec_event[i]["event_code"] in ec_pair["event_end"]) and (
                            ec_event[i + 1]["event_code"] in ec_pair["event_end"]
                        ):
                            remove_rows.append(i + 1)
                        i += 1

                    # drop redundant/repeating events ie [1 0 0 0 1 0]
                    df_ec = (
                        df_ec.with_row_index()
                        .filter(~pl.col("index").is_in(remove_rows))
                        .drop("index")
                    )

                # If df starts with event_end code, delete to start with event start code
                # Currently cannot handle orphan end events
                if df_ec["event_code"].first() in ec_pair["event_end"]:
                    df_ec = df_ec.slice(offset=1)

                # ================================================
                #                Concat Logic
                # ================================================
                # filter out events that dont have end events
                # ie repeated event codes 1,7,1,7,1,1,7
                # the 1 did not have a closing 7 event code
                # TODO: highlight non closing event codes in table
                df_ec = df_ec.filter(
                    pl.col("event_code")
                    != pl.col("event_code").shift(-1, fill_value=True)
                )

                # Separate all start event codes
                df_start = df_ec.filter(
                    pl.col("event_code") == ec_pair["event_start"],
                    pl.col("parameter") == param,
                )

                # Separate all end event codes
                df_end = df_ec.filter(
                    pl.col("event_code").is_in(ec_pair["event_end"]),
                    pl.col("parameter") == param,
                ).rename(lambda cname: cname + "2")

                # # If Event does not have pair, skip without matching
                # if df_start.is_empty() or df_end.is_empty():
                #     print("----- event codes with no matches -----")
                #     print(f"ec1: {ec_pair[0]}, ec2: {ec_pair[1]}: param: {param} ")
                #     continue

                # Stack horizontally
                df_temp = (
                    df_start.hstack(df_end)
                    # .with_columns(duration=pl.col("dt2") - pl.col("dt"))
                    # .with_columns(pl.col("duration").dt.total_milliseconds() / 1000)
                )
                nonFlash_holder.append(df_temp)

    return pl.concat(items=nonFlash_holder, how="vertical")
