import io
import os
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import polars as pl
import utils

uri = os.getenv("URI")
app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    # TODO: set correct origins for production
    # allow_origins=["http://127.0.0.1:5500"],
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================================
#                Test Endpoint
# ================================================


@app.get("/")
async def root():
    return {"message": "Hello, Root!"}


# ===========================
#  * Load Event code data
# ===========================
# TODO: save to db to deploy on server or package in docker
# event codes with descriptions
ec = pl.read_csv(source="api/event_codes.csv")
# event code pairs
ec_pairs = pl.read_csv("api/event_pairs.csv").rows()
# single event codes
ec_singles = pl.read_csv("api/event_singles.csv")["event_code"].to_list()

# ================================================
# *             Locations
# Used to populate locations dropdown
# ================================================


@app.get("/form_locids")
async def get_locations() -> list[dict]:
    """Return list of atms_id, location name dictionaries
    for select dropdown"""

    qry = """SELECT atms_id, name FROM intersection"""
    df = pl.read_database_uri(query=qry, uri=uri, engine="adbc").sort("name")

    # NOTE: anotther way to acheive results
    # w/o list, only dictionary
    # df = dict(zip(df["atms_id"], df["name"]))
    return df.to_dicts()


# ================================================
# *               Hi-res Function
# get and process hi-res data into dataframe
# ================================================


def process_hires(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
) -> pl.DataFrame:

    # retun filtered list of files from directory
    dir_list, path = utils.filter_directory(locid, date, time, addhrs)
    if not dir_list:
        return pl.DataFrame()

    # read files and add event descriptors
    df_holder: list[pl.DataFrame] = utils.add_event_descriptors(dir_list, path)

    # Concat list of dataframes to one dataframe for processing
    df_data: pl.DataFrame = (
        pl.concat(df_holder).sort(by="dt")
        # Use series to map values from df to another df, great feature!!
        .with_columns(
            event_descriptor=pl.col("event_code").replace_strict(
                old=ec["event_code"], new=ec["event_descriptor"], default="x"
            )
        )
    )

    # ================================================
    # *             Pair Event Code
    #  alarms that have paired event codes for on/off
    # ================================================
    eventdf_holder = utils.pair_events(ec_pairs, df_data)

    # ================================================
    #  *             Single Event Code
    #   Events that only have singel event code
    # ================================================

    df_singles = utils.single_events(ec_singles, df_data)

    eventdf_holder.append(df_singles)

    df_fin: pl.DataFrame = (
        pl.concat(eventdf_holder)
        .sort(by="dt")
        .select(pl.lit(locid).alias("loc_id"), pl.all())
    ).with_columns(
        pl.col("dt").dt.strftime(r"%Y-%m-%d %H:%M:%S%.3f"),
        pl.col("dt2").dt.strftime(r"%Y-%m-%d %H:%M:%S%.3f"),
        pl.col("duration").round(1),
    )

    # df_fin.write_csv("api/test_results.csv")

    return df_fin


@app.get("/purdue")
async def get_purdue(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
) -> StreamingResponse:

    df_hres = process_hires(locid=locid, date=date, time=time, addhrs=addhrs)

    stream = io.StringIO()
    df_hres.write_csv(stream)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


# ================================================
# *        Endpoint for HiRes Timeline
# ================================================


@app.get("/timeline_viz")
async def get_purdue(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
) -> dict:

    df_hres = process_hires(locid=locid, date=date, time=time, addhrs=addhrs)

    df_viz = df_hres.with_columns(
        pl.col("dt").dt.timestamp("ms"), pl.col("dt2").dt.timestamp("ms")
    )

    series = []
    colors = []

    ring_ec = {
        "Green": (1, 7, "#00E396"),
        "Yellow Clr": (8, 9, "#FEB019"),
        "Red Clr": (10, 11, "#FF4560"),
    }
    # TODO: how do i get this info for each intersection as ring structures vary?
    phases = [
        (1, "R1"),
        (2, "R1"),
        (3, "R1"),
        (4, "R1"),
        (5, "R2"),
        (6, "R2"),
        (7, "R2"),
        (8, "R2"),
    ]
    num_records = 0
    for k, v in ring_ec.items():

        # TODO: how do i add the phase here to name without another nested for loop?
        for phase in phases:
            df = df_viz.filter(
                (pl.col("event_code") == v[0])
                & (pl.col("event_code2") == v[1])
                & (pl.col("parameter") == phase[0])
            )
            # df.write_csv("api/test_results.csv")

            df = df.with_columns(
                y=pl.concat_list("dt", "dt2"), x=pl.lit(phase[1])
            ).select(["x", "y"])

            # df.write_json("api/test_results.json")

            temp_d = {"name": f"Ph {phase[0]} {k}"}
            num_records += len(df)
            temp_d["data"] = df.to_dicts()
            series.append(temp_d)
            colors.append(v[2])

    # with open("api/res.json", "w") as f:
    #     for items in series:
    #         f.write("%s\n" % items)
    print(num_records)

    res = dict()
    res["series"] = series
    res["colors"] = colors
    return res


# ================================================
# *            Hi-res AG grid endpoint
# Populate hi-res data in ag grid
# ================================================


@app.get("/hiresgrid")
async def get_hires_grid(
    locid: str,
    date: str,
    time: str | None = "0000",
    addhrs: str | None = "1",
) -> list[dict]:

    df_hres = process_hires(locid=locid, date=date, time=time, addhrs=addhrs)
    # print(df_hres.columns)

    return df_hres.to_dicts()
