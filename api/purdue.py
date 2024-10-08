import os
import polars as pl


# ===========================
#  Read event code descriptors
#
#
# ===========================

# event codes
ec = pl.read_csv(source="api/event_codes.csv")


# ===========================
#      User Input
#
#   locid, date, time, # hours
# ===========================

id_input = input("ENTER Intersection ID: ")
locid = ((5 - len(id_input)) * "0") + id_input

# locate directory with files and create list
path = os.getenv("DIRECTORY") + locid
dir_list = os.listdir(path)
# print(dir_list)

# Enter nothing to process all date/times
id_date = input("Enter date to process (ex. 2024-08-04): ")
if id_date != "":
    id_date = id_date.replace("-", "_")

    id_hour = input("Enter start time (ex. 1400): ")
    if id_hour == "":
        id_hour = "0000"

    add_hours = input("Enter number of hours to process: ")

    file_name = f"TRAF_{locid}_{id_date}_{id_hour}.csv"
    idx = dir_list.index(file_name)

    if add_hours == "":
        idx_end = None
    else:
        idx_end = idx + int(add_hours)

    dir_list = dir_list[idx:idx_end]


print(dir_list)

# ===========================
#      Read Csv Data
#
#   Create one df from selected files
#   Add event descriptor to each event
# ===========================

df_holder = []


for file in dir_list:
    print(file)
    df = pl.read_csv(
        source=path + "\\" + file,
        has_header=False,
        skip_rows=6,
        new_columns=["dt", "event_code", "parameter"],
    )

    # ===========================
    #      Clean/Format data
    #
    #   add event descriptor names
    # ===========================

    df = df.with_columns(
        pl.col("dt").str.to_datetime(r"%-m/%d/%Y %H:%M:%S%.3f"),
        pl.col("event_code").str.replace_all(" ", ""),
        pl.col("parameter").str.replace_all(" ", ""),
        # location_id=pl.lit(locid),
    ).with_columns(
        pl.col("event_code").str.to_integer(),
        pl.col("parameter").str.to_integer(),
    )

    df_holder.append(df)

# Concat all data
df_data: pl.DataFrame = (
    pl.concat(df_holder).sort(by="dt")
    # Use series to map values from df to another df, great feature!!
    .with_columns(
        event_descriptor=pl.col("event_code").replace_strict(
            old=ec["event_code"], new=ec["event_descriptor"], default="x"
        )
    )
)

# Write out data as is with descriptors, no pairing
df_data.write_csv(f"Proc_{file}")


# ================================================
#            Paired Event Code
#
#  alarms that have seperate event codes for on/off
#
# ================================================

ec_pairs = pl.read_csv("api/event_pairs.csv").rows()
eventdf_holder = []


for ec_pair in ec_pairs:

    ec_params = df_data.filter(pl.col("event_code") == ec_pair[0])["parameter"].unique()

    for param in ec_params:
        # print(f"ec1: {ec_pair[0]}, ec2: {ec_pair[1]}: param: {param} ")

        df_ec = df_data.filter(
            pl.col("event_code").is_in([ec_pair[0], ec_pair[1]]),
            pl.col("parameter") == param,
        )
        # df_ec.write_csv("df_ec1.csv")

        df_ec = df_ec.filter(
            pl.col("event_code") != pl.col("event_code").shift(1, fill_value=ec_pair[1])
        )

        # df_ec.write_csv("df_ec2.csv")
        # print(df_ec)

        # Delete last row if ends with start pair
        if df_ec["event_code"].item(df_ec.height - 1) == ec_pair[0]:
            df_ec = df_ec.slice(0, df_ec.height - 1)

        df_start = df_ec.filter(
            pl.col("event_code") == ec_pair[0], pl.col("parameter") == param
        )

        df_end = df_ec.filter(
            pl.col("event_code") == ec_pair[1], pl.col("parameter") == param
        ).rename(lambda cname: cname + "2")

        # If Event does not have pair, skip without matching
        if df_start.is_empty() or df_end.is_empty():
            print(f"ec1: {ec_pair[0]}, ec2: {ec_pair[1]}: param: {param} ")
            continue

        # print(df_start)
        # print(df_end)

        df_temp = (
            df_start.hstack(df_end)
            .with_columns(duration=pl.col("dt2") - pl.col("dt"))
            .with_columns(pl.col("duration").dt.total_milliseconds() / 1000)
        )

        eventdf_holder.append(df_temp)

# ================================================
#                Single Event Code
#
#
#   Events that only have singel event code
#
# ================================================

ec_singles = pl.read_csv("api/event_singles.csv")
ec_singles = ec_singles["event_code"].to_list()


# for ec_single in ec_singles:

#     print(f"ec1: {ec_single[0]}, descriptor: {ec_single[1]} ")

df_es = df_data.filter(pl.col("event_code").is_in(ec_singles))
df_es2 = df_es.rename(lambda cname: cname + "2")


df_temp = (
    df_es.hstack(df_es2)
    # added .1 seconds to be able to display on timeline chart, for now leave off
    # .with_columns(pl.col("dt2") + pl.duration(milliseconds=100))
    .with_columns(duration=pl.col("dt2") - pl.col("dt")).with_columns(
        pl.col("duration").dt.total_milliseconds() / 1000
    )
)
# df_temp.write_csv("temp.csv")

eventdf_holder.append(df_temp)

df_fin: pl.DataFrame = (
    pl.concat(eventdf_holder)
    .sort(by="dt")
    .select(pl.lit(locid).alias("loc_id"), pl.all())
)

print(df_fin.schema)
# TODO: write without T in datetime, same for api
df_fin.write_csv(f"./api/{locid}_{id_date}_{id_hour}_results.csv")
